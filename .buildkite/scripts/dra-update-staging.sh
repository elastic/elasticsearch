#!/bin/bash

set -euo pipefail

WORKFLOW="${DRA_WORKFLOW:-snapshot}"

# Don't publish main branch to staging
if [[ "$BUILDKITE_BRANCH" == "main" && "$WORKFLOW" == "staging" ]]; then
  exit 0
fi

RM_BRANCH="$BUILDKITE_BRANCH"
if [[ "$BUILDKITE_BRANCH" == "main" ]]; then
  RM_BRANCH=master
fi

BEATS_MANIFEST=$(curl -sS "https://artifacts-staging.elastic.co/beats/latest/${RM_BRANCH}.json" | jq -r '.manifest_url')
ML_MANIFEST=$(curl -sS "https://artifacts-staging.elastic.co/ml-cpp/latest/${RM_BRANCH}.json" | jq -r '.manifest_url')
ES_MANIFEST=$(curl -sS "https://artifacts-staging.elastic.co/elasticsearch/latest/${RM_BRANCH}.json" | jq -r '.manifest_url')

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
  # TODO do it
  echo 1
fi
