#!/bin/bash

set -euo pipefail

# Usage: .buildkite/scripts/third-party-test-credentials.gcs.sh <path/to/write/credentials.json>

source .buildkite/scripts/setup-legacy-vault.sh

vault read -field=private_key_data gcp-elastic-ci-prod/key/elasticsearch-ci-thirdparty-gcs | base64 --decode > "$1"
