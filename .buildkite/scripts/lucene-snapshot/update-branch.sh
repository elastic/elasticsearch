#!/bin/bash

set -euo pipefail

if [[ "$BUILDKITE_BRANCH" != "lucene_snapshot" ]]; then
  echo "Error: This script should only be run on the lucene_snapshot branch"
  exit 1
fi

echo --- Updating lucene_snapshot branch with main

git config --global user.name elasticsearchmachine
git config --global user.email 'infra-root+elasticsearchmachine@elastic.co'

git checkout lucene_snapshot
git fetch origin main
git merge --no-edit origin/main
git push origin lucene_snapshot
