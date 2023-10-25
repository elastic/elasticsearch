#!/bin/bash

set -euo pipefail

echo "steps:"

source .buildkite/scripts/branches.sh

for BRANCH in "${BRANCHES[@]}"; do
  INTAKE_PIPELINE_SLUG="elasticsearch-intake"
  BUILD_JSON=$(curl -sH "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds?branch=${BRANCH}&state=passed&per_page=1" | jq '.[0] | {commit: .commit, url: .web_url}')
  LAST_GOOD_COMMIT=$(echo "${BUILD_JSON}" | jq -r '.commit')

  cat <<EOF
  - trigger: elasticsearch-periodic
    label: Trigger periodic pipeline for $BRANCH
    async: true
    build:
      branch: "$BRANCH"
      commit: "$LAST_GOOD_COMMIT"
  - trigger: elasticsearch-periodic-packaging
    label: Trigger periodic-packaging pipeline for $BRANCH
    async: true
    build:
      branch: "$BRANCH"
      commit: "$LAST_GOOD_COMMIT"
  - trigger: elasticsearch-periodic-platform-support
    label: Trigger periodic-platform-support pipeline for $BRANCH
    async: true
    build:
      branch: "$BRANCH"
      commit: "$LAST_GOOD_COMMIT"
EOF
done
