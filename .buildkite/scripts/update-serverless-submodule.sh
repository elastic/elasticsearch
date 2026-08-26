#!/bin/bash

set -euo pipefail

INTAKE_PIPELINE_SLUG="elasticsearch-intake"
BUILD_JSON=$(curl -sH "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds?branch=main&state=passed&per_page=1" | jq '.[0] | {commit: .commit, url: .web_url}')
LAST_GOOD_COMMIT=$(echo "${BUILD_JSON}" | jq -r '.commit')

# Do not trigger a submodule update that would move the serverless submodule
# backwards (or nowhere). Right after a linked stateful + serverless merge, the
# serverless submodule can already point at a commit newer than the latest one
# that passed intake; triggering here would run the serverless tests against
# stale elasticsearch code and get passing tests muted on main.
if [[ "$(.buildkite/scripts/serverless-submodule-advance-decision.sh "${LAST_GOOD_COMMIT}")" == "skip" ]]; then
  echo "Skipping submodule update: latest passing intake commit ${LAST_GOOD_COMMIT} would not move the serverless submodule forward."
  exit 0
fi

cat <<EOF | buildkite-agent pipeline upload
steps:
  - trigger: elasticsearch-serverless-validate-submodule
    label: ":elasticsearch: Update elasticsearch submodule in serverless"
    build:
      message: "Elasticsearch submodule update build"
      env:
        ELASTICSEARCH_SUBMODULE_COMMIT: "${LAST_GOOD_COMMIT}"
EOF
