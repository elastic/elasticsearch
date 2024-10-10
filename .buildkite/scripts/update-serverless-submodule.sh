#!/bin/bash

set -euo pipefail

INTAKE_PIPELINE_SLUG="elasticsearch-intake"
BUILD_JSON=$(curl -sH "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds?branch=main&state=passed&per_page=1" | jq '.[0] | {commit: .commit, url: .web_url}')
LAST_GOOD_COMMIT=$(echo "${BUILD_JSON}" | jq -r '.commit')

cat <<EOF | buildkite-agent pipeline upload
steps:
  - trigger: elasticsearch-serverless-validate-submodule
    label: ":elasticsearch: Update elasticsearch submodule in serverless"
    build:
      message: "Elasticsearch submodule update build"
      env:
        ELASTICSEARCH_SUBMODULE_COMMIT: "${LAST_GOOD_COMMIT}"
EOF
