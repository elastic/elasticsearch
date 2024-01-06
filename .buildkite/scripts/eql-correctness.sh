#!/bin/bash

set -euo pipefail

eql_test_credentials_file="$(pwd)/x-pack/plugin/eql/qa/correctness/credentials.gcs.json"
export eql_test_credentials_file
vault read -field=credentials.gcs.json secret/ci/elastic-elasticsearch/migrated/eql_test_credentials > "${eql_test_credentials_file}"

.ci/scripts/run-gradle.sh -Dignore.tests.seed :x-pack:plugin:eql:qa:correctness:check
