# Steam historical capture - public automation files

Este repositorio contiene solo codigo de captura y un manifest de AppIDs/nombres
publicos de Steam. No deben subirse credenciales, `.env`, archivos SQLite de
captura ni identificadores personales.

## Flujo

1. GitHub Actions captura Steam a un SQLite temporal con Azure apagado.
2. Reintenta faltantes de Store.
3. Se autentica contra Azure mediante OIDC.
4. Enciende Azure Database for PostgreSQL.
5. Autoriza temporalmente solo la IP del runner.
6. Sube el batch y lo valida.
7. Elimina la regla temporal y detiene Azure en un job de cleanup.

Los secretos se configuran en GitHub Settings > Secrets and variables > Actions.
