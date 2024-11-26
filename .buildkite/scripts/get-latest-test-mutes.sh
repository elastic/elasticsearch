#!/bin/bash

if [[ ! "${BUILDKITE_PULL_REQUEST:-}" || "${BUILDKITE_AGENT_META_DATA_PROVIDER:-}" == "k8s" ]]; then
  exit 0
fi

testMuteBranch="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}"
testMuteFile="$(mktemp)"

# If this PR contains changes to muted-tests.yml, we disable this functionality
# Otherwise, we wouldn't be able to test unmutes
# if [[ ! $(gh pr diff "$BUILDKITE_PULL_REQUEST" --name-only | grep 'muted-tests.yml') ]]; then # TODO add back before PR review/merge
  echo "Getting latest muted tests..."
  gh api -H 'Accept: application/vnd.github.v3.raw' "repos/elastic/elasticsearch/contents/muted-tests.yml?ref=$testMuteBranch" > "$testMuteFile"
  echo "Saved to file: $testMuteFile"
  cat "$testMuteFile"

  if [[ -s "$testMuteFile" ]]; then
    mkdir -p ~/.gradle
    # This is using gradle.properties instead of an env var so that it's easily compatible with the Windows pre-command hook
    echo "org.gradle.project.org.elasticsearch.additional.muted.tests=$testMuteFile" >> ~/.gradle/gradle.properties
    cat ~/.gradle/gradle.properties
  fi
# fi
