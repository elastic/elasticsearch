#!/bin/bash

set -euo pipefail

source .buildkite/scripts/branches.sh

for BRANCH in "${BRANCHES[@]}"; do
  # Don't publish main branch to staging
  if [[ "$BRANCH" == "main" ]]; then
    continue
  fi

  echo "--- Checking $BRANCH"

  BEATS_MANIFEST=$(curl -sS "https://artifacts-staging.elastic.co/beats/latest/${BRANCH}.json" | jq -r '.manifest_url')
  ML_MANIFEST=$(curl -sS "https://artifacts-staging.elastic.co/ml-cpp/latest/${BRANCH}.json" | jq -r '.manifest_url')
  ES_MANIFEST=$(curl -sS "https://artifacts-staging.elastic.co/elasticsearch/latest/${BRANCH}.json" | jq -r '.manifest_url')

  ES_BEATS_DEPENDENCY=$(curl -sS "$ES_MANIFEST" | jq -r '.projects.elasticsearch.dependencies[] | select(.prefix == "beats") | .build_uri')
  ES_ML_DEPENDENCY=$(curl -sS "$ES_MANIFEST" | jq -r '.projects.elasticsearch.dependencies[] | select(.prefix == "ml-cpp") | .build_uri')

  SHOULD_TRIGGER=""

  if [ "$BEATS_MANIFEST" = "$ES_BEATS_DEPENDENCY" ]; then
    echo "ES has the latest beats"
  else
    echo "Need to trigger a build, $BEATS_MANIFEST available but ES has $ES_BEATS_DEPENDENCY"
    SHOULD_TRIGGER=true
  fi

  if [ "$ML_MANIFEST" = "$ES_ML_DEPENDENCY" ]; then
    echo "ES has the latest ml-cpp"
  else
    echo "Need to trigger a build, $ML_MANIFEST available but ES has $ES_ML_DEPENDENCY"
    SHOULD_TRIGGER=true
  fi

  if [[ "$SHOULD_TRIGGER" == "true" ]]; then
    echo "Triggering DRA staging workflow for $BRANCH"
    cat << EOF | buildkite-agent pipeline upload
steps:
  - trigger: elasticsearch-dra-workflow
    label: Trigger DRA staging workflow for $BRANCH
    async: true
    build:
      branch: "$BRANCH"
      env:
        DRA_WORKFLOW: staging
EOF
  fi
done
