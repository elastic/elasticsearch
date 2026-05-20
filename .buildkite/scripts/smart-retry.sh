#!/bin/bash

# Smart retry test filtering — see .buildkite/scripts/smart-retry/ for details.
# Reads: BUILDKITE_API_TOKEN, BUILDKITE_JOB_ID, BUILDKITE_PIPELINE_SLUG,
#         BUILDKITE_BUILD_NUMBER, ORIGIN_JOB_ID, TESTS_SEED
# Writes: .failed-test-history.json, buildkite-agent metadata and annotations

if ! command -v bun > /dev/null; then
  echo --- Installing bun
  npm install -g bun@1.0.4
fi

bun .buildkite/scripts/smart-retry/main.ts
