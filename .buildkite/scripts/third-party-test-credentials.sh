#!/bin/bash

set -euo pipefail

# You'll notice that most of the variables are exported twice with different names here
# The first/uppercase export is to ensure that Buildkite masks the values in the logs should they accidentally be output
# The second/lowercase export is what the tests expect/require

if [[ "${USE_3RD_PARTY_AZURE_CREDENTIALS:-}" == "true" ]]; then
  # These credentials expire periodically and must be manually renewed - the process is in the onboarding/process docs.
  json=$(vault read -format=json secret/ci/elastic-elasticsearch/migrated/azure_thirdparty_test_creds)

  AZURE_STORAGE_ACCOUNT_SECRET=$(echo "$json" | jq -r .data.account_id)
  export AZURE_STORAGE_ACCOUNT_SECRET
  export azure_storage_account="$AZURE_STORAGE_ACCOUNT_SECRET"

  AZURE_STORAGE_KEY=$(echo "$json" | jq -r .data.account_key)
  export AZURE_STORAGE_KEY
  export azure_storage_key="$AZURE_STORAGE_KEY"
fi

if [[ "${USE_3RD_PARTY_AZURE_SAS_CREDENTIALS:-}" == "true" ]]; then
  # These credentials expire periodically and must be manually renewed - the process is in the onboarding/process docs.
  json=$(vault read -format=json secret/ci/elastic-elasticsearch/migrated/azure_thirdparty_sas_test_creds)

  AZURE_STORAGE_ACCOUNT_SECRET=$(echo "$json" | jq -r .data.account_id)
  export AZURE_STORAGE_ACCOUNT_SECRET
  export azure_storage_account="$AZURE_STORAGE_ACCOUNT_SECRET"

  AZURE_STORAGE_SAS_TOKEN=$(echo "$json" | jq -r .data.account_sas_token)
  export AZURE_STORAGE_SAS_TOKEN
  export azure_storage_sas_token="$AZURE_STORAGE_SAS_TOKEN"
fi

if [[ "${USE_3RD_PARTY_S3_CREDENTIALS:-}" == "true" ]]; then
  json=$(.buildkite/scripts/get-legacy-secret.sh aws-test/creds/elasticsearch-ci-s3)
  AMAZON_S3_ACCESS_KEY=$(echo "$json" | jq -r .data.access_key)
  export AMAZON_S3_ACCESS_KEY
  export amazon_s3_access_key="$AMAZON_S3_ACCESS_KEY"

  AMAZON_S3_SECRET_KEY=$(echo "$json" | jq -r .data.secret_key)
  export AMAZON_S3_SECRET_KEY
  export amazon_s3_secret_key="$AMAZON_S3_SECRET_KEY"
fi

if [[ "${USE_3RD_PARTY_GCS_CREDENTIALS:-}" == "true" ]]; then
  export google_storage_service_account=$(mktemp)
  .buildkite/scripts/third-party-test-credentials.gcs.sh "$google_storage_service_account"
fi

if [[ "${USE_3RD_PARTY_MS_GRAPH_CREDENTIALS:-}" == "true" ]]; then
  json=$(vault read -format=json secret/ci/elastic-elasticsearch/ms_graph_thirdparty_test_creds)

  MS_GRAPH_TENANT_ID=$(echo "$json" | jq -r .data.tenant_id)
  export ms_graph_tenant_id="$MS_GRAPH_TENANT_ID"

  MS_GRAPH_CLIENT_ID=$(echo "$json" | jq -r .data.client_id)
  export ms_graph_client_id="$MS_GRAPH_CLIENT_ID"

  MS_GRAPH_CLIENT_SECRET=$(echo "$json" | jq -r .data.client_secret)
  export ms_graph_client_secret="$MS_GRAPH_CLIENT_SECRET"

  MS_GRAPH_USERNAME=$(echo "$json" | jq -r .data.username)
  export ms_graph_username="$MS_GRAPH_USERNAME"

  MS_GRAPH_GROUP_ID=$(echo "$json" | jq -r .data.group_id)
  export ms_graph_group_id="$MS_GRAPH_GROUP_ID"
fi

unset json
