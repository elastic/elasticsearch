#!/bin/bash

# Smart retry test filtering — see .buildkite/scripts/smart-retry/ for details.
# Reads: BUILDKITE_API_TOKEN, BUILDKITE_JOB_ID, BUILDKITE_PIPELINE_SLUG,
#         BUILDKITE_BUILD_NUMBER, ORIGIN_JOB_ID, TESTS_SEED
# Writes: .failed-test-history.json, buildkite-agent metadata and annotations

# Resolve paths relative to this script's location, not the caller's CWD. This
# script is sourced cross-repo, so a CWD-relative path would not resolve.
SMART_RETRY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
node "${SMART_RETRY_DIR}/smart-retry/main.ts"
