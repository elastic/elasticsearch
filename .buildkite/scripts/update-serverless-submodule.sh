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
#
# Best-effort: elasticsearch-serverless also guards this in its
# validate-submodule pipeline, so if we cannot determine the comparison here we
# fall through and trigger as before. gh authenticates via GH_TOKEN/GITHUB_TOKEN.
CURRENT_SUBMODULE_COMMIT=$(gh api "repos/elastic/elasticsearch-serverless/contents/elasticsearch?ref=main" --jq '.sha' 2>/dev/null || true)

if [[ "${CURRENT_SUBMODULE_COMMIT}" =~ ^[0-9a-f]{40}$ ]]; then
  COMPARE_STATUS=$(gh api "repos/elastic/elasticsearch/compare/${CURRENT_SUBMODULE_COMMIT}...${LAST_GOOD_COMMIT}" --jq '.status' 2>/dev/null || true)

  if [[ "${COMPARE_STATUS}" == "behind" || "${COMPARE_STATUS}" == "identical" ]]; then
    echo "Latest passing intake commit ${LAST_GOOD_COMMIT} is not ahead of the current serverless submodule ${CURRENT_SUBMODULE_COMMIT} (status: ${COMPARE_STATUS}); skipping submodule update."
    exit 0
  fi
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
