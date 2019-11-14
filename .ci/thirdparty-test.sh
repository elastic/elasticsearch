#!/bin/bash

set -euo pipefail

export amazon_s3_bucket=elasticsearch-ci.us-west-2
export amazon_s3_base_path=$BRANCH
export google_storage_bucket=elasticsearch-ci-thirdparty
export google_storage_base_path=$BRANCH
export azure_storage_container=elasticsearch-ci-thirdparty
export azure_storage_base_path=$BRANCH

# TODO: Move the vault secrets handling in the build script to make it easy to test

#Azure

set +x
VAULT_TOKEN=$(vault write -field=token auth/approle/login role_id="$VAULT_ROLE_ID" secret_id="$VAULT_SECRET_ID")
export VAULT_TOKEN
export data=$(vault read -format=json secret/elasticsearch-ci/azure_thirdparty_test_creds)
export azure_storage_account=$(echo $data | jq -r .data.account_id)
export azure_storage_key=$(echo $data | jq -r .data.account_key)
unset data
set -x

# GCS

export google_storage_service_account=$(pwd)/gcs_service_account.json
set +x
vault read \
   -field=private_key_data \
   gcp-elastic-ci-prod/key/elasticsearch-ci-thirdparty-gcs | base64 --decode > "$google_storage_service_account"
set -x

# S3

set +x
export data=$(vault read -format=json aws-test/creds/elasticsearch-ci-s3)
export amazon_s3_access_key=$(echo $data | jq -r .data.access_key)
export amazon_s3_secret_key=$(echo $data | jq -r .data.secret_key)
unset data
set -x

unset VAULT_TOKEN

./.ci/build.sh :plugins:repository-gcs:check :plugins:repository-s3:check :plugins:repository-azure:check

#  Azure SAS
set +x
VAULT_TOKEN=$(vault write -field=token auth/approle/login role_id=$VAULT_ROLE_ID secret_id=$VAULT_SECRET_ID)
export VAULT_TOKEN
export data=$(vault read -format=json secret/elasticsearch-ci/azure_thirdparty_sas_test_creds)
export azure_storage_account=$(echo $data | jq -r .data.account_id)
export azure_storage_sas_token=$(echo $data | jq -r .data.account_sas_token)
unset VAULT_TOKEN data
set -x
export azure_storage_container=elasticsearch-ci-thirdparty-sas
./.ci/build.sh :plugins:repository-azure:check



