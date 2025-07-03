#!/bin/sh
# Stop script for start-local
# More information: https://github.com/elastic/start-local
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}"
docker compose stop
