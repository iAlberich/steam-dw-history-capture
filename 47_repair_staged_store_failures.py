from __future__ import annotations

import os
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

COUNTRY_CODE = os.getenv("STEAM_COUNTRY_CODE", "us")
LANGUAGE = os.getenv("STEAM_LANGUAGE", "en")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "25"))

REPAIR_STORE_INTERVAL = float(
    os.getenv("REPAIR_STORE_INTERVAL_SECONDS", "2.0")
)
REPAIR_MAX_ATTEMPTS = int(
    os.getenv("REPAIR_MAX_ATTEMPTS", "4")
)

STORE_DETAILS_URL = "https://store.steampowered.com/api/appdetails"


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
            pool_connections=4,
            pool_maxsize=4,
        ),
    )

    s.headers.update(
        {
            "User-Agent": (
                "SteamDWAcademicHistoricalRepair/1.0 "
                "(university BI project; controlled requests)"
            ),
            "Accept": "application/json,text/plain,*/*",
        }
    )

    return s


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
) -> dict[str, Any]:
    r = s.get(
        STORE_DETAILS_URL,
        params={
            "appids": appid,
            "cc": COUNTRY_CODE,
            "l": LANGUAGE,
        },
        timeout=REQUEST_TIMEOUT,
    )

    if r.status_code in (403, 429):
        raise RuntimeError(f"HTTP {r.status_code}")

    r.raise_for_status()
    return parse_store(r.json(), appid, known_is_free)


def retryable(message: str) -> bool:
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


def rebuild_batch_status(db: sqlite3.Connection) -> tuple[int, str, str | None]:
    rows = db.execute(
        """
        SELECT store_error, reviews_error, players_error
        FROM game_capture
        """
    ).fetchall()

    error_games = 0
    values: list[str] = []

    for store_error, reviews_error, players_error in rows:
        has_error = False

        for source, message in (
            ("store", store_error),
            ("reviews", reviews_error),
            ("players", players_error),
        ):
            if message:
                has_error = True
                values.append(f"{source}: {message}")

        if has_error:
            error_games += 1

    status = "SUCCESS" if error_games == 0 else "PARTIAL"

    error_message = None
    if values:
        c = Counter(values)
        error_message = " | ".join(
            f"{count}x {message}"
            for message, count in c.most_common(30)
        )

    db.execute(
        """
        UPDATE capture_batch
        SET
            error_games = ?,
            status = ?,
            error_message = ?
        """,
        (error_games, status, error_message),
    )
    db.commit()

    return error_games, status, error_message


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Uso: python 47_repair_staged_store_failures.py "
            "captures\\steam_capture_....sqlite"
        )

    path = Path(sys.argv[1])

    if not path.exists():
        raise RuntimeError(f"No existe: {path}")

    db = sqlite3.connect(path)

    try:
        pending = db.execute(
            """
            SELECT
                appid,
                name,
                known_is_free,
                store_error
            FROM game_capture
            WHERE store_ok = 0
            ORDER BY appid
            """
        ).fetchall()

        print("=" * 78)
        print("REPAIR STAGED - STORE")
        print("=" * 78)
        print(f"archivo:              {path}")
        print(f"faltantes Store:       {len(pending)}")
        print(f"intervalo:             {REPAIR_STORE_INTERVAL:.2f}s")
        print(f"max intentos/juego:    {REPAIR_MAX_ATTEMPTS}")
        print("=" * 78)

        if not pending:
            error_games, status, _ = rebuild_batch_status(db)
            print("No hay precios pendientes.")
            print(f"errores finales: {error_games}")
            print(f"estado: {status}")
            return

        s = create_session()
        recovered = 0

        for idx, (appid, name, known_is_free, old_error) in enumerate(
            pending,
            start=1,
        ):
            if idx > 1:
                time.sleep(REPAIR_STORE_INTERVAL)

            last_error = old_error or "sin detalle"

            for attempt in range(1, REPAIR_MAX_ATTEMPTS + 1):
                try:
                    data = fetch_store(
                        s,
                        int(appid),
                        bool(known_is_free),
                    )

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
                            int(appid),
                        ),
                    )
                    db.commit()

                    recovered += 1
                    last_error = ""
                    print(
                        f"[{idx:02}/{len(pending):02}] "
                        f"{appid:<8} | RECOVERED | {name[:48]}"
                    )
                    break

                except Exception as exc:
                    last_error = f"{type(exc).__name__}: {exc}"

                    if (
                        not retryable(last_error)
                        or attempt == REPAIR_MAX_ATTEMPTS
                    ):
                        break

                    cooldown = min(60.0, 15.0 * (2 ** (attempt - 1)))
                    print(
                        f"[{idx:02}/{len(pending):02}] "
                        f"{appid:<8} | retry {attempt}/"
                        f"{REPAIR_MAX_ATTEMPTS} -> {last_error}; "
                        f"espera {cooldown:.0f}s"
                    )
                    time.sleep(cooldown)

            if last_error:
                db.execute(
                    """
                    UPDATE game_capture
                    SET store_error = ?
                    WHERE appid = ?
                    """,
                    (last_error, int(appid)),
                )
                db.commit()

                print(
                    f"[{idx:02}/{len(pending):02}] "
                    f"{appid:<8} | STILL_FAILED | {last_error}"
                )

        error_games, status, error_message = rebuild_batch_status(db)

        remaining_store = db.execute(
            "SELECT COUNT(*) FROM game_capture WHERE store_ok = 0"
        ).fetchone()[0]

        print()
        print("=" * 78)
        print("REPAIR TERMINADO")
        print("=" * 78)
        print(f"intentados:        {len(pending)}")
        print(f"recuperados:       {recovered}")
        print(f"Store restantes:   {remaining_store}")
        print(f"errores finales:   {error_games}")
        print(f"estado del batch:  {status}")

        if error_message:
            print(f"detalle:           {error_message}")

        print()
        print("AZURE NO FUE UTILIZADO.")

    finally:
        db.close()


if __name__ == "__main__":
    main()
