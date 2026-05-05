#!/bin/bash

set -euo pipefail

export VAULT_CLIENT_TIMEOUT="${VAULT_CLIENT_TIMEOUT:-30s}"

VAULT_ROLE_ID=$(vault read -field=role-id secret/ci/elastic-elasticsearch/legacy-vault-credentials)
VAULT_SECRET_ID=$(vault read -field=secret-id secret/ci/elastic-elasticsearch/legacy-vault-credentials)
VAULT_ADDR=https://secrets.elastic.co:8200

unset VAULT_TOKEN
VAULT_TOKEN=$(vault write -field=token auth/approle/login role_id=$VAULT_ROLE_ID secret_id=$VAULT_SECRET_ID)
export VAULT_TOKEN
