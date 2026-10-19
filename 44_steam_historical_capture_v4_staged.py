from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

# ============================================================
# STEAM HISTORICAL CAPTURE v4 STAGED
#
# Basada en v3 BALANCED:
# - mismo rate limiter compartido para appdetails + appreviews
# - mismo retry diferido por fuente
# - NO abre PostgreSQL ni Azure durante las consultas a Steam
# - guarda la captura en un SQLite local temporal
#
# Luego 45_upload_staged_capture_to_azure.py carga el batch.
# ============================================================

COUNTRY_CODE = os.getenv("STEAM_COUNTRY_CODE", "us")
LANGUAGE = os.getenv("STEAM_LANGUAGE", "en")
CAPTURE_LIMIT = int(os.getenv("CAPTURE_LIMIT", "0"))

HIST_WORKERS = max(1, min(int(os.getenv("HIST_WORKERS", "8")), 12))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "25"))

STORE_DOMAIN_INTERVAL = float(
    os.getenv("STORE_DOMAIN_INTERVAL_SECONDS", "0.35")
)
PLAYERS_INTERVAL = float(
    os.getenv("PLAYERS_INTERVAL_SECONDS", "0.05")
)

DEFERRED_RETRY_WAIT = float(
    os.getenv("DEFERRED_RETRY_WAIT_SECONDS", "20")
)
DEFERRED_STORE_DOMAIN_INTERVAL = float(
    os.getenv("DEFERRED_STORE_DOMAIN_INTERVAL_SECONDS", "0.85")
)
DEFERRED_PLAYERS_INTERVAL = float(
    os.getenv("DEFERRED_PLAYERS_INTERVAL_SECONDS", "0.20")
)
DEFERRED_MAX_ATTEMPTS = int(
    os.getenv("DEFERRED_MAX_ATTEMPTS", "3")
)

MANIFEST_PATH = Path(
    os.getenv("GAME_MANIFEST_PATH", "steam_games_manifest.json")
)
OUTPUT_DIR = Path(
    os.getenv("CAPTURE_OUTPUT_DIR", "captures")
)

STORE_DETAILS_URL = "https://store.steampowered.com/api/appdetails"
REVIEWS_URL = "https://store.steampowered.com/appreviews/{appid}"
PLAYERS_URL = (
    "https://api.steampowered.com/"
    "ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
)

_tls = threading.local()


class RateGate:
    def __init__(self, min_interval: float):
        self.min_interval = max(0.0, min_interval)
        self.lock = threading.Lock()
        self.last_start = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            delay = max(0.0, self.min_interval - (now - self.last_start))
            if delay > 0:
                time.sleep(delay)
            self.last_start = time.monotonic()


store_domain_gate = RateGate(STORE_DOMAIN_INTERVAL)
players_gate = RateGate(PLAYERS_INTERVAL)
deferred_store_domain_gate = RateGate(DEFERRED_STORE_DOMAIN_INTERVAL)
deferred_players_gate = RateGate(DEFERRED_PLAYERS_INTERVAL)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat()


def create_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=1.0,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    s.mount(
        "https://",
        HTTPAdapter(
            max_retries=retry,
            pool_connections=16,
            pool_maxsize=16,
        ),
    )
    s.headers.update(
        {
            "User-Agent": (
                "SteamDWAcademicHistoricalCapture/4.0 "
                "(university BI project; controlled requests)"
            ),
            "Accept": "application/json,text/plain,*/*",
        }
    )
    return s


def get_thread_session() -> requests.Session:
    s = getattr(_tls, "session", None)
    if s is None:
        s = create_session()
        _tls.session = s
    return s


def get_json(
    s: requests.Session,
    url: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    r = s.get(url, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code in (403, 429):
        raise RuntimeError(f"HTTP {r.status_code}")
    r.raise_for_status()
    return r.json()


def parse_store(
    payload: dict[str, Any],
    appid: int,
    known_is_free: bool,
) -> dict[str, Any]:
    wrapper = payload.get(str(appid), {})
    if not wrapper.get("success"):
        raise RuntimeError("appdetails success=false")

    data = wrapper.get("data") or {}
    price = data.get("price_overview") or {}

    api_is_free = data.get("is_free")
    is_free = known_is_free if api_is_free is None else bool(api_is_free)

    if is_free:
        return {
            "initial_price_cents": 0,
            "current_price_cents": 0,
            "discount_percent": 0,
        }

    currency = price.get("currency")
    if currency not in (None, "USD"):
        raise RuntimeError(f"currency inesperada: {currency}")

    return {
        "initial_price_cents": price.get("initial"),
        "current_price_cents": price.get("final"),
        "discount_percent": price.get("discount_percent"),
    }


def fetch_store(
    s: requests.Session,
    appid: int,
    known_is_free: bool,
    *,
    deferred: bool = False,
) -> dict[str, Any]:
    gate = deferred_store_domain_gate if deferred else store_domain_gate
    gate.wait()
    payload = get_json(
        s,
        STORE_DETAILS_URL,
        {"appids": appid, "cc": COUNTRY_CODE, "l": LANGUAGE},
    )
    return parse_store(payload, appid, known_is_free)


def fetch_reviews(
    s: requests.Session,
    appid: int,
    *,
    deferred: bool = False,
) -> dict[str, Any]:
    gate = deferred_store_domain_gate if deferred else store_domain_gate
    gate.wait()
    payload = get_json(
        s,
        REVIEWS_URL.format(appid=appid),
        {
            "json": 1,
            "filter": "all",
            "language": "all",
            "review_type": "all",
            "purchase_type": "all",
            "num_per_page": 1,
        },
    )
    if payload.get("success") != 1:
        raise RuntimeError("reviews success != 1")

    q = payload.get("query_summary") or {}
    positive = q.get("total_positive")
    negative = q.get("total_negative")
    total = q.get("total_reviews")

    ratio = None
    if isinstance(positive, int) and isinstance(total, int) and total > 0:
        ratio = round(positive * 100.0 / total, 4)

    return {
        "review_score": q.get("review_score"),
        "total_positive_reviews": positive,
        "total_negative_reviews": negative,
        "total_reviews": total,
        "positive_review_ratio_pct": ratio,
    }


def fetch_players(
    s: requests.Session,
    appid: int,
    *,
    deferred: bool = False,
) -> dict[str, Any]:
    gate = deferred_players_gate if deferred else players_gate
    gate.wait()
    payload = get_json(s, PLAYERS_URL, {"appid": appid})
    response = payload.get("response") or {}
    if response.get("result") != 1:
        raise RuntimeError(
            f"current players result={response.get('result')}"
        )
    count = response.get("player_count")
    return {
        "current_players": int(count) if count is not None else None
    }


def capture_one(
    appid: int,
    name: str,
    known_is_free: bool,
) -> dict[str, Any]:
    s = get_thread_session()
    result = {
        "appid": appid,
        "name": name,
        "known_is_free": known_is_free,
        "captured_at_utc": utc_now(),
        "store": None,
        "reviews": None,
        "players": None,
        "store_error": None,
        "reviews_error": None,
        "players_error": None,
    }

    try:
        result["store"] = fetch_store(s, appid, known_is_free)
    except Exception as exc:
        result["store_error"] = f"{type(exc).__name__}: {exc}"

    try:
        result["reviews"] = fetch_reviews(s, appid)
    except Exception as exc:
        result["reviews_error"] = f"{type(exc).__name__}: {exc}"

    try:
        result["players"] = fetch_players(s, appid)
    except Exception as exc:
        result["players_error"] = f"{type(exc).__name__}: {exc}"

    return result


def is_retryable(message: str | None) -> bool:
    if not message:
        return False
    m = message.lower()
    return any(
        token in m
        for token in (
            "http 429",
            "http 403",
            "timeout",
            "timed out",
            "connection",
            "502",
            "503",
            "504",
        )
    )


def summarize_errors(
    states: dict[int, dict[str, str | None]],
) -> str | None:
    values: list[str] = []
    for state in states.values():
        for source in ("store", "reviews", "players"):
            msg = state.get(source)
            if msg:
                values.append(f"{source}: {msg}")
    if not values:
        return None

    c = Counter(values)
    return " | ".join(
        f"{count}x {message}"
        for message, count in c.most_common(30)
    )


def retry_one_source(
    source: str,
    appid: int,
    known_is_free: bool,
) -> dict[str, Any]:
    s = get_thread_session()
    last_error: Exception | None = None

    for attempt in range(1, DEFERRED_MAX_ATTEMPTS + 1):
        try:
            if source == "store":
                return fetch_store(
                    s, appid, known_is_free, deferred=True
                )
            if source == "reviews":
                return fetch_reviews(s, appid, deferred=True)
            if source == "players":
                return fetch_players(s, appid, deferred=True)
            raise RuntimeError(f"Fuente no soportada: {source}")
        except Exception as exc:
            last_error = exc
            message = f"{type(exc).__name__}: {exc}"
            if not is_retryable(message):
                break
            if attempt < DEFERRED_MAX_ATTEMPTS:
                time.sleep(min(5.0, 1.5 * attempt))

    raise last_error or RuntimeError("Retry fallo")


def load_manifest() -> tuple[list[tuple[int, str, bool]], dict[str, Any]]:
    if not MANIFEST_PATH.exists():
        raise RuntimeError(
            f"No existe {MANIFEST_PATH}. "
            "Ejecuta primero 43_export_game_manifest.py."
        )

    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    raw_games = data.get("games") or []

    games = [
        (int(x["appid"]), str(x["name"]), bool(x["is_free"]))
        for x in raw_games
    ]
    games.sort(key=lambda x: x[0])

    if CAPTURE_LIMIT > 0:
        games = games[:CAPTURE_LIMIT]
    elif len(games) != 1000:
        raise RuntimeError(
            f"Captura completa exige 1000 juegos; manifest tiene {len(games)}."
        )

    return games, data


def init_stage_db(
    path: Path,
    batch_id: str,
    started_at: datetime,
    expected_games: int,
    manifest: dict[str, Any],
) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript(
        """
        CREATE TABLE capture_batch (
            batch_id TEXT PRIMARY KEY,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT,
            expected_games INTEGER NOT NULL,
            captured_games INTEGER NOT NULL DEFAULT 0,
            error_games INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            error_message TEXT,
            country_code TEXT NOT NULL,
            currency_code TEXT NOT NULL
        );

        CREATE TABLE game_capture (
            appid INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            known_is_free INTEGER NOT NULL,
            captured_at_utc TEXT NOT NULL,

            initial_price_cents INTEGER,
            current_price_cents INTEGER,
            discount_percent INTEGER,

            review_score INTEGER,
            total_positive_reviews INTEGER,
            total_negative_reviews INTEGER,
            total_reviews INTEGER,
            positive_review_ratio_pct REAL,

            current_players INTEGER,

            store_ok INTEGER NOT NULL DEFAULT 0,
            reviews_ok INTEGER NOT NULL DEFAULT 0,
            players_ok INTEGER NOT NULL DEFAULT 0,

            store_error TEXT,
            reviews_error TEXT,
            players_error TEXT
        );
        """
    )
    db.execute(
        """
        INSERT INTO capture_batch (
            batch_id, started_at_utc, expected_games,
            captured_games, error_games, status,
            country_code, currency_code
        )
        VALUES (?, ?, ?, 0, 0, 'STARTED', ?, ?)
        """,
        (
            batch_id,
            iso(started_at),
            expected_games,
            str(manifest.get("country_code") or COUNTRY_CODE),
            str(manifest.get("currency_code") or "USD"),
        ),
    )
    db.commit()
    return db


def save_result(db: sqlite3.Connection, result: dict[str, Any]) -> None:
    s = result["store"] or {}
    r = result["reviews"] or {}
    p = result["players"] or {}

    db.execute(
        """
        INSERT INTO game_capture (
            appid, name, known_is_free, captured_at_utc,
            initial_price_cents, current_price_cents, discount_percent,
            review_score, total_positive_reviews, total_negative_reviews,
            total_reviews, positive_review_ratio_pct,
            current_players,
            store_ok, reviews_ok, players_ok,
            store_error, reviews_error, players_error
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            result["appid"],
            result["name"],
            int(result["known_is_free"]),
            iso(result["captured_at_utc"]),
            s.get("initial_price_cents"),
            s.get("current_price_cents"),
            s.get("discount_percent"),
            r.get("review_score"),
            r.get("total_positive_reviews"),
            r.get("total_negative_reviews"),
            r.get("total_reviews"),
            r.get("positive_review_ratio_pct"),
            p.get("current_players"),
            int(result["store"] is not None),
            int(result["reviews"] is not None),
            int(result["players"] is not None),
            result["store_error"],
            result["reviews_error"],
            result["players_error"],
        ),
    )
    db.commit()


def update_retry(
    db: sqlite3.Connection,
    appid: int,
    source: str,
    data: dict[str, Any],
) -> None:
    if source == "store":
        db.execute(
            """
            UPDATE game_capture
            SET
                initial_price_cents = ?,
                current_price_cents = ?,
                discount_percent = ?,
                store_ok = 1,
                store_error = NULL
            WHERE appid = ?
            """,
            (
                data.get("initial_price_cents"),
                data.get("current_price_cents"),
                data.get("discount_percent"),
                appid,
            ),
        )
    elif source == "reviews":
        db.execute(
            """
            UPDATE game_capture
            SET
                review_score = ?,
                total_positive_reviews = ?,
                total_negative_reviews = ?,
                total_reviews = ?,
                positive_review_ratio_pct = ?,
                reviews_ok = 1,
                reviews_error = NULL
            WHERE appid = ?
            """,
            (
                data.get("review_score"),
                data.get("total_positive_reviews"),
                data.get("total_negative_reviews"),
                data.get("total_reviews"),
                data.get("positive_review_ratio_pct"),
                appid,
            ),
        )
    elif source == "players":
        db.execute(
            """
            UPDATE game_capture
            SET
                current_players = ?,
                players_ok = 1,
                players_error = NULL
            WHERE appid = ?
            """,
            (data.get("current_players"), appid),
        )
    else:
        raise RuntimeError(f"Fuente no soportada: {source}")

    db.commit()


def update_retry_error(
    db: sqlite3.Connection,
    appid: int,
    source: str,
    message: str,
) -> None:
    column = {
        "store": "store_error",
        "reviews": "reviews_error",
        "players": "players_error",
    }[source]
    db.execute(
        f"UPDATE game_capture SET {column} = ? WHERE appid = ?",
        (message, appid),
    )
    db.commit()


def finalize_batch(
    db: sqlite3.Connection,
    batch_id: str,
    finished_at: datetime,
    captured_games: int,
    error_games: int,
    status: str,
    error_message: str | None,
) -> None:
    db.execute(
        """
        UPDATE capture_batch
        SET
            finished_at_utc = ?,
            captured_games = ?,
            error_games = ?,
            status = ?,
            error_message = ?
        WHERE batch_id = ?
        """,
        (
            iso(finished_at),
            captured_games,
            error_games,
            status,
            error_message,
            batch_id,
        ),
    )
    db.commit()


def main() -> None:
    started_wall = utc_now()
    started_mono = time.monotonic()

    games, manifest = load_manifest()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    batch_id = str(uuid.uuid4())
    stamp = started_wall.strftime("%Y%m%dT%H%M%SZ")
    out_path = OUTPUT_DIR / f"steam_capture_{stamp}_{batch_id[:8]}.sqlite"

    db = init_stage_db(
        out_path,
        batch_id,
        started_wall,
        len(games),
        manifest,
    )

    captured_games = 0
    executor: ThreadPoolExecutor | None = None

    try:
        print("STEAM HISTORICAL CAPTURE v4 STAGED")
        print("=" * 96)
        print(f"batch_id:                      {batch_id}")
        print(f"archivo:                       {out_path}")
        print(f"juegos:                        {len(games):,}")
        print(f"workers:                       {HIST_WORKERS}")
        print(f"store-domain shared interval:  {STORE_DOMAIN_INTERVAL:.2f}s")
        print(f"players interval:              {PLAYERS_INTERVAL:.2f}s")
        print(f"deferred store-domain:         {DEFERRED_STORE_DOMAIN_INTERVAL:.2f}s")
        print("=" * 96)
        print()

        states: dict[int, dict[str, str | None]] = {}
        deferred: list[tuple[str, int, str, bool]] = []

        executor = ThreadPoolExecutor(
            max_workers=HIST_WORKERS,
            thread_name_prefix="steam-history-v4",
        )

        futures = {
            executor.submit(capture_one, appid, name, is_free):
            (appid, name, is_free)
            for appid, name, is_free in games
        }

        for idx, future in enumerate(as_completed(futures), start=1):
            appid, name, is_free = futures[future]

            try:
                result = future.result()
            except Exception as exc:
                message = f"worker {type(exc).__name__}: {exc}"
                result = {
                    "appid": appid,
                    "name": name,
                    "known_is_free": is_free,
                    "captured_at_utc": utc_now(),
                    "store": None,
                    "reviews": None,
                    "players": None,
                    "store_error": message,
                    "reviews_error": message,
                    "players_error": message,
                }

            save_result(db, result)
            captured_games += 1

            states[appid] = {
                "store": result["store_error"],
                "reviews": result["reviews_error"],
                "players": result["players_error"],
            }

            for source in ("store", "reviews", "players"):
                message = states[appid][source]
                if message and is_retryable(message):
                    deferred.append(
                        (source, appid, name, is_free)
                    )

            status = (
                "OK" if not any(states[appid].values()) else "PARTIAL"
            )
            print(
                f"[{idx:04}/{len(games):04}] "
                f"{appid:<8} | {status:<7} | {name[:44]}"
            )

        executor.shutdown(wait=True)
        executor = None

        print()
        print("-" * 96)
        print(f"Retries diferidos pendientes: {len(deferred)}")

        if deferred:
            by_source = Counter(x[0] for x in deferred)
            print(
                "  " + " | ".join(
                    f"{source}={count}"
                    for source, count in sorted(by_source.items())
                )
            )
            print(
                f"Esperando {DEFERRED_RETRY_WAIT:.0f}s "
                "antes de iniciar recuperacion..."
            )
            time.sleep(DEFERRED_RETRY_WAIT)

            recovered = Counter()

            for idx, (
                source, appid, name, is_free
            ) in enumerate(deferred, start=1):
                try:
                    data = retry_one_source(
                        source, appid, is_free
                    )
                    update_retry(db, appid, source, data)
                    states[appid][source] = None
                    recovered[source] += 1
                    print(
                        f"[RETRY {idx:03}/{len(deferred):03}] "
                        f"{source:<7} {appid:<8} | RECOVERED"
                    )
                except Exception as exc:
                    message = f"{type(exc).__name__}: {exc}"
                    states[appid][source] = message
                    update_retry_error(
                        db, appid, source, message
                    )
                    print(
                        f"[RETRY {idx:03}/{len(deferred):03}] "
                        f"{source:<7} {appid:<8} | STILL_FAILED"
                    )

            print()
            print(
                "Recuperados: "
                + " | ".join(
                    f"{source}={count}"
                    for source, count in sorted(recovered.items())
                )
            )

        unresolved_games = sum(
            1 for state in states.values() if any(state.values())
        )

        status = (
            "SUCCESS"
            if captured_games == len(games) and unresolved_games == 0
            else "PARTIAL"
            if captured_games > 0
            else "FAILED"
        )

        finished_wall = utc_now()
        finalize_batch(
            db,
            batch_id,
            finished_wall,
            captured_games,
            unresolved_games,
            status,
            summarize_errors(states),
        )

        elapsed_min = (time.monotonic() - started_mono) / 60.0

        print()
        print("=" * 96)
        print("CAPTURA v4 STAGED TERMINADA")
        print("=" * 96)
        print(f"batch_id:       {batch_id}")
        print(f"archivo:        {out_path}")
        print(f"capturados:     {captured_games}/{len(games)}")
        print(f"errores finales:{unresolved_games}")
        print(f"estado:         {status}")
        print(f"duracion:       {elapsed_min:.2f} min")
        print()
        print("AZURE NO FUE UTILIZADO DURANTE ESTA CAPTURA.")
        print("El siguiente paso es subir este .sqlite con el script 45.")

    except KeyboardInterrupt:
        finished_wall = utc_now()
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
            executor = None

        finalize_batch(
            db,
            batch_id,
            finished_wall,
            captured_games,
            0,
            "PARTIAL" if captured_games else "FAILED",
            "Interrumpido manualmente.",
        )
        print("\nCaptura interrumpida; checkpoint SQLite conservado.")
        raise

    finally:
        if executor is not None:
            executor.shutdown(wait=True)
        db.close()


if __name__ == "__main__":
    main()
