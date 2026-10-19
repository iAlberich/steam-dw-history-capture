from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import psycopg


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return value


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Uso: python 49_validate_uploaded_batch.py "
            "captures/steam_capture_....sqlite"
        )

    stage_path = Path(sys.argv[1])
    if not stage_path.exists():
        raise RuntimeError(f"No existe staged file: {stage_path}")

    sdb = sqlite3.connect(stage_path)
    sdb.row_factory = sqlite3.Row
    try:
        batch = sdb.execute(
            "SELECT * FROM capture_batch LIMIT 1"
        ).fetchone()
        if batch is None:
            raise RuntimeError("No existe capture_batch en staged file.")

        expected_game = int(batch["captured_games"])
        expected_price = int(
            sdb.execute(
                "SELECT COUNT(*) FROM game_capture WHERE store_ok=1"
            ).fetchone()[0]
        )
        expected_player = int(
            sdb.execute(
                "SELECT COUNT(*) FROM game_capture"
            ).fetchone()[0]
        )
        expected_players_ok = int(
            sdb.execute(
                "SELECT COUNT(*) FROM game_capture WHERE players_ok=1"
            ).fetchone()[0]
        )
        expected_review = int(
            sdb.execute(
                "SELECT COUNT(*) FROM game_capture WHERE reviews_ok=1"
            ).fetchone()[0]
        )
        batch_id = str(batch["batch_id"])
        staged_status = str(batch["status"])
        staged_errors = int(batch["error_games"])
    finally:
        sdb.close()

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
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    cr.capture_run_id,
                    cr.expected_games,
                    cr.captured_games,
                    cr.error_games,
                    cr.status,
                    COUNT(DISTINCT gs.snapshot_id) AS game_snapshots,
                    COUNT(DISTINCT ps.snapshot_id) AS price_snapshots,
                    COUNT(DISTINCT pls.snapshot_id) AS player_snapshots,
                    COUNT(DISTINCT rs.snapshot_id) AS review_snapshots,
                    COUNT(DISTINCT pls.snapshot_id)
                        FILTER (WHERE pls.players_ok IS TRUE) AS players_ok
                FROM ods_final.capture_run cr
                LEFT JOIN ods_final.game_snapshot gs
                  ON gs.capture_run_id = cr.capture_run_id
                LEFT JOIN ods_final.price_snapshot ps
                  ON ps.snapshot_id = gs.snapshot_id
                LEFT JOIN ods_final.player_snapshot pls
                  ON pls.snapshot_id = gs.snapshot_id
                LEFT JOIN ods_final.review_snapshot rs
                  ON rs.snapshot_id = gs.snapshot_id
                WHERE cr.source_batch_id = %s::uuid
                GROUP BY cr.capture_run_id
                """,
                (batch_id,),
            )
            row = cur.fetchone()

            if row is None:
                raise RuntimeError(
                    f"No se encontro batch {batch_id} en Azure."
                )

            (
                run_id,
                expected_games,
                captured_games,
                error_games,
                status,
                game_n,
                price_n,
                player_n,
                review_n,
                players_ok_n,
            ) = row

            checks = {
                "capture_run.expected_games": (
                    int(expected_games), int(batch["expected_games"])
                ),
                "capture_run.captured_games": (
                    int(captured_games), expected_game
                ),
                "capture_run.error_games": (
                    int(error_games), staged_errors
                ),
                "game_snapshot": (int(game_n), expected_game),
                "price_snapshot": (int(price_n), expected_price),
                "player_snapshot": (int(player_n), expected_player),
                "review_snapshot": (int(review_n), expected_review),
                "players_ok": (int(players_ok_n), expected_players_ok),
            }

            bad = {
                name: values
                for name, values in checks.items()
                if values[0] != values[1]
            }

            if str(status) != staged_status:
                bad["status"] = (str(status), staged_status)

            if bad:
                details = "; ".join(
                    f"{k}: azure={v[0]} expected={v[1]}"
                    for k, v in bad.items()
                )
                raise RuntimeError(
                    "Validacion posterior a carga fallo: " + details
                )

            cur.execute(
                """
                SELECT COUNT(*)
                FROM ods_final.capture_run
                WHERE source_batch_id = %s::uuid
                """,
                (batch_id,),
            )
            duplicate_count = int(cur.fetchone()[0])
            if duplicate_count != 1:
                raise RuntimeError(
                    f"source_batch_id aparece {duplicate_count} veces."
                )

        print("=" * 76)
        print("VALIDACION AZURE OK")
        print("=" * 76)
        print(f"capture_run_id: {run_id}")
        print(f"batch_id:       {batch_id}")
        print(f"game_snapshot:  {game_n}")
        print(f"price_snapshot: {price_n}")
        print(f"player_snapshot:{player_n}")
        print(f"review_snapshot:{review_n}")
        print(f"players_ok:     {players_ok_n}")
        print(f"status:         {status}")
        print("duplicados:     0")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
