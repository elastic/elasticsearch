#!/bin/bash

set -euo pipefail

if [[ "$BUILDKITE_BRANCH" != "lucene_snapshot"* ]]; then
  echo "Error: This script should only be run on lucene_snapshot branches"
  exit 1
fi

echo --- Updating "$BUILDKITE_BRANCH" branch with main

git config --global user.name elasticsearchmachine
git config --global user.email 'infra-root+elasticsearchmachine@elastic.co'

git checkout "$BUILDKITE_BRANCH"
git fetch origin main
git merge --no-edit origin/main
git push origin "$BUILDKITE_BRANCH"
