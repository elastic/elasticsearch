#!/bin/bash

set -euo pipefail

case "$BUILDKITE_BRANCH" in
  lucene_snapshot)
    UPSTREAM="main"
    ;;
  lucene_snapshot_11)
    UPSTREAM="lucene_snapshot"
    ;;
  *)
    echo "Error: no upstream branch configured for [$BUILDKITE_BRANCH]"
    exit 1
    ;;
esac

echo --- Updating "$BUILDKITE_BRANCH" branch with "$UPSTREAM"

git config --global user.name elasticsearchmachine
git config --global user.email 'infra-root+elasticsearchmachine@elastic.co'

git checkout "$BUILDKITE_BRANCH"
git fetch origin "$UPSTREAM"
git merge --no-edit "origin/$UPSTREAM"
git push origin "$BUILDKITE_BRANCH"
