from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv()


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return value


def load_stage(path: Path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row

    batch = db.execute(
        "SELECT * FROM capture_batch LIMIT 1"
    ).fetchone()

    if batch is None:
        db.close()
        raise RuntimeError("El archivo staged no contiene capture_batch.")

    rows = db.execute(
        "SELECT * FROM game_capture ORDER BY appid"
    ).fetchall()

    return db, batch, rows


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Uso: python 45_upload_staged_capture_to_azure.py "
            "captures\\steam_capture_....sqlite"
        )

    path = Path(sys.argv[1])
    if not path.exists():
        raise RuntimeError(f"No existe: {path}")

    stage_db, batch, rows = load_stage(path)

    try:
        if batch["status"] not in ("SUCCESS", "PARTIAL"):
            raise RuntimeError(
                f"Batch no finalizado correctamente: status={batch['status']}"
            )

        if len(rows) != int(batch["captured_games"]):
            raise RuntimeError(
                "Conteo staged inconsistente: "
                f"game_capture={len(rows)} vs "
                f"captured_games={batch['captured_games']}"
            )

        conn = psycopg.connect(
            host=required("AZURE_PGHOST"),
            port=int(os.getenv("AZURE_PGPORT", "5432")),
            dbname=required("AZURE_PGDATABASE"),
            user=required("AZURE_PGUSER"),
            password=required("AZURE_PGPASSWORD"),
            sslmode=os.getenv("AZURE_PGSSLMODE", "require"),
            connect_timeout=15,
        )

        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    # Preflight schema
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_schema='ods_final'
                              AND table_name='capture_run'
                              AND column_name='source_batch_id'
                        )
                        """
                    )
                    if not cur.fetchone()[0]:
                        raise RuntimeError(
                            "Falta source_batch_id. "
                            "Ejecuta primero 42_prepare_azure_capture_batches.sql"
                        )

                    cur.execute(
                        """
                        SELECT capture_run_id
                        FROM ods_final.capture_run
                        WHERE source_batch_id = %s::uuid
                        """,
                        (batch["batch_id"],),
                    )
                    existing = cur.fetchone()
                    if existing:
                        print(
                            "Este batch ya fue cargado anteriormente. "
                            f"capture_run_id={existing[0]}"
                        )
                        return

                    appids = [int(r["appid"]) for r in rows]
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM ods_final.game
                        WHERE active_monitoring IS TRUE
                          AND appid = ANY(%s)
                        """,
                        (appids,),
                    )
                    present = int(cur.fetchone()[0])
                    if present != len(appids):
                        raise RuntimeError(
                            f"Azure contiene {present}/{len(appids)} "
                            "AppIDs activos del batch."
                        )

                    cur.execute(
                        """
                        SELECT market_id
                        FROM ods_final.market
                        WHERE LOWER(country_code)=LOWER(%s)
                          AND UPPER(currency_code)=UPPER(%s)
                        LIMIT 1
                        """,
                        (
                            batch["country_code"],
                            batch["currency_code"],
                        ),
                    )
                    market = cur.fetchone()
                    if not market:
                        raise RuntimeError("Mercado del batch no existe en Azure.")
                    market_id = int(market[0])

                    cur.execute(
                        """
                        INSERT INTO ods_final.capture_run (
                            source_batch_id,
                            started_at_utc,
                            finished_at_utc,
                            expected_games,
                            captured_games,
                            error_games,
                            status,
                            error_message
                        )
                        VALUES (
                            %s::uuid, %s::timestamptz, %s::timestamptz,
                            %s, %s, %s, %s, %s
                        )
                        RETURNING capture_run_id
                        """,
                        (
                            batch["batch_id"],
                            batch["started_at_utc"],
                            batch["finished_at_utc"],
                            batch["expected_games"],
                            batch["captured_games"],
                            batch["error_games"],
                            batch["status"],
                            batch["error_message"],
                        ),
                    )
                    run_id = int(cur.fetchone()[0])

                    cur.executemany(
                        """
                        INSERT INTO ods_final.game_snapshot (
                            capture_run_id, appid, captured_at_utc
                        )
                        VALUES (%s, %s, %s::timestamptz)
                        """,
                        [
                            (
                                run_id,
                                int(r["appid"]),
                                r["captured_at_utc"],
                            )
                            for r in rows
                        ],
                    )

                    cur.execute(
                        """
                        SELECT appid, snapshot_id
                        FROM ods_final.game_snapshot
                        WHERE capture_run_id = %s
                        """,
                        (run_id,),
                    )
                    snapshot_by_appid = {
                        int(appid): int(snapshot_id)
                        for appid, snapshot_id in cur.fetchall()
                    }

                    price_rows = []
                    review_rows = []
                    player_rows = []

                    for r in rows:
                        snapshot_id = snapshot_by_appid[int(r["appid"])]

                        if int(r["store_ok"]):
                            price_rows.append(
                                (
                                    snapshot_id,
                                    market_id,
                                    r["initial_price_cents"],
                                    r["current_price_cents"],
                                    r["discount_percent"],
                                )
                            )

                        if int(r["reviews_ok"]):
                            review_rows.append(
                                (
                                    snapshot_id,
                                    r["review_score"],
                                    r["total_positive_reviews"],
                                    r["total_negative_reviews"],
                                    r["total_reviews"],
                                    r["positive_review_ratio_pct"],
                                )
                            )

                        player_rows.append(
                            (
                                snapshot_id,
                                r["current_players"],
                                bool(r["players_ok"]),
                            )
                        )

                    if price_rows:
                        cur.executemany(
                            """
                            INSERT INTO ods_final.price_snapshot (
                                snapshot_id, market_id,
                                initial_price_cents,
                                current_price_cents,
                                discount_percent
                            )
                            VALUES (%s,%s,%s,%s,%s)
                            """,
                            price_rows,
                        )

                    if review_rows:
                        cur.executemany(
                            """
                            INSERT INTO ods_final.review_snapshot (
                                snapshot_id,
                                review_score,
                                total_positive_reviews,
                                total_negative_reviews,
                                total_reviews,
                                positive_review_ratio_pct
                            )
                            VALUES (%s,%s,%s,%s,%s,%s)
                            """,
                            review_rows,
                        )

                    cur.executemany(
                        """
                        INSERT INTO ods_final.player_snapshot (
                            snapshot_id,
                            current_players,
                            players_ok
                        )
                        VALUES (%s,%s,%s)
                        """,
                        player_rows,
                    )

                    # Validacion dentro de la misma transaccion
                    cur.execute(
                        """
                        SELECT
                            COUNT(DISTINCT gs.snapshot_id),
                            COUNT(DISTINCT ps.snapshot_id),
                            COUNT(DISTINCT pls.snapshot_id),
                            COUNT(DISTINCT rs.snapshot_id)
                        FROM ods_final.game_snapshot gs
                        LEFT JOIN ods_final.price_snapshot ps
                          ON ps.snapshot_id = gs.snapshot_id
                        LEFT JOIN ods_final.player_snapshot pls
                          ON pls.snapshot_id = gs.snapshot_id
                        LEFT JOIN ods_final.review_snapshot rs
                          ON rs.snapshot_id = gs.snapshot_id
                        WHERE gs.capture_run_id = %s
                        """,
                        (run_id,),
                    )
                    game_n, price_n, player_n, review_n = map(
                        int, cur.fetchone()
                    )

                    expected_price = sum(int(r["store_ok"]) for r in rows)
                    expected_review = sum(int(r["reviews_ok"]) for r in rows)

                    if game_n != len(rows):
                        raise RuntimeError(
                            f"Validacion fallo game_snapshot: {game_n}/{len(rows)}"
                        )
                    if price_n != expected_price:
                        raise RuntimeError(
                            f"Validacion fallo price: {price_n}/{expected_price}"
                        )
                    if player_n != len(rows):
                        raise RuntimeError(
                            f"Validacion fallo players: {player_n}/{len(rows)}"
                        )
                    if review_n != expected_review:
                        raise RuntimeError(
                            f"Validacion fallo reviews: {review_n}/{expected_review}"
                        )

            print("=" * 76)
            print("BATCH CARGADO CORRECTAMENTE EN AZURE")
            print("=" * 76)
            print(f"capture_run_id: {run_id}")
            print(f"batch_id:       {batch['batch_id']}")
            print(f"game_snapshot:  {game_n}")
            print(f"price_snapshot: {price_n}")
            print(f"player_snapshot:{player_n}")
            print(f"review_snapshot:{review_n}")
            print(f"status:         {batch['status']}")
            print()
            print("Ya puedes DETENER Azure.")

        finally:
            conn.close()

    finally:
        stage_db.close()


if __name__ == "__main__":
    main()
