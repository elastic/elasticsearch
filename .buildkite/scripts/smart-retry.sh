#!/bin/bash

# Smart retry test filtering — see .buildkite/scripts/smart-retry/ for details.
# Reads: BUILDKITE_API_TOKEN, BUILDKITE_JOB_ID, BUILDKITE_PIPELINE_SLUG,
#         BUILDKITE_BUILD_NUMBER, ORIGIN_JOB_ID, TESTS_SEED
# Writes: .failed-test-history.json, buildkite-agent metadata and annotations

# Node.js may be unavailable on platforms with old glibc (see setup_node.sh).
# Skip smart retry gracefully in that case; the step will use a fresh seed.
if ! command -v node > /dev/null 2>&1; then
  echo "Skipping smart retry: Node.js is not available on this platform"
  return 0 2>/dev/null || exit 0
fi

# Resolve paths relative to this script's location, not the caller's CWD. This
# script is sourced cross-repo, so a CWD-relative path would not resolve.
SMART_RETRY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
node "${SMART_RETRY_DIR}/smart-retry/main.ts"
