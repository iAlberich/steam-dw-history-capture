from __future__ import annotations

import os
import time

import psycopg


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return value


def main() -> None:
    attempts = int(os.getenv("AZURE_DB_CONNECT_ATTEMPTS", "30"))
    wait_seconds = float(os.getenv("AZURE_DB_CONNECT_WAIT_SECONDS", "10"))

    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            conn = psycopg.connect(
                host=required("AZURE_PGHOST"),
                port=int(os.getenv("AZURE_PGPORT", "5432")),
                dbname=required("AZURE_PGDATABASE"),
                user=required("AZURE_PGUSER"),
                password=required("AZURE_PGPASSWORD"),
                sslmode=os.getenv("AZURE_PGSSLMODE", "require"),
                connect_timeout=10,
            )
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    if cur.fetchone()[0] != 1:
                        raise RuntimeError("SELECT 1 no devolvio 1.")
            finally:
                conn.close()

            print(f"Azure PostgreSQL accesible en intento {attempt}/{attempts}.")
            return

        except Exception as exc:
            last_error = exc
            print(
                f"Intento {attempt}/{attempts}: PostgreSQL aun no accesible: "
                f"{type(exc).__name__}: {exc}"
            )
            if attempt < attempts:
                time.sleep(wait_seconds)

    raise RuntimeError(
        "Azure PostgreSQL no estuvo accesible dentro del tiempo esperado."
    ) from last_error


if __name__ == "__main__":
    main()
