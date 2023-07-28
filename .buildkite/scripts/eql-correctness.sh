#!/bin/bash

set -euo pipefail

set +x
#VAULT_TOKEN=$(vault write -field=token auth/approle/login role_id=$VAULT_ROLE_ID secret_id=$VAULT_SECRET_ID)
export VAULT_TOKEN
export eql_test_credentials_file="$(pwd)/x-pack/plugin/eql/qa/correctness/credentials.gcs.json"
vault read -field=credentials.gcs.json secret/ci/elastic-elasticsearch/migrated/eql_test_credentials > ${eql_test_credentials_file}
#unset VAULT_TOKEN
set -x

.ci/scripts/run-gradle.sh -Dignore.tests.seed :x-pack:plugin:eql:qa:correctness:check