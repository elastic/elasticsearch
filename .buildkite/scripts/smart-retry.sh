#!/bin/bash

# Smart retry test filtering — see .buildkite/scripts/smart-retry/ for details.
# Reads: BUILDKITE_API_TOKEN, BUILDKITE_JOB_ID, BUILDKITE_PIPELINE_SLUG,
#         BUILDKITE_BUILD_NUMBER, ORIGIN_JOB_ID, TESTS_SEED
# Writes: .failed-test-history.json, buildkite-agent metadata and annotations

# setup_node.sh exports SKIP_NODE_SETUP=true on platforms where Node.js 24
# cannot run (glibc < 2.25). Skip smart retry gracefully with a Buildkite
# annotation and metadata so the omission is visible in the build.
# Any other reason for node to be absent is treated as an unexpected failure.
if [[ "${SKIP_NODE_SETUP:-false}" == "true" ]]; then
  reason="Node.js unavailable: glibc too old for Node.js 24"
  echo "Skipping smart retry: $reason"
  # Buildkite keys annotations by context per build, so a constant context would
  # let each skipping job overwrite the previous one. Scope it to the job, as
  # smart-retry/main.ts does.
  buildkite-agent annotate --style info --context "smart-retry-${BUILDKITE_JOB_ID:-unknown}" \
    "Smart retry unavailable: $reason. Tests will run with a fresh seed." 2>/dev/null || true
  # Same metadata shape smart-retry.ts emits, so both paths are searchable alike.
  buildkite-agent meta-data set smart-retry-status "disabled" 2>/dev/null || true
  buildkite-agent meta-data set smart-retry-disabled-reason "node-unavailable" 2>/dev/null || true
  buildkite-agent meta-data set smart-retry-details "$reason" 2>/dev/null || true
  return 0 2>/dev/null || exit 0
fi

if ! command -v node > /dev/null 2>&1; then
  echo "ERROR: Node.js expected on PATH but not found; check setup_node.sh"
  exit 1
fi

# Resolve paths relative to this script's location, not the caller's CWD. This
# script is sourced cross-repo, so a CWD-relative path would not resolve.
SMART_RETRY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
node "${SMART_RETRY_DIR}/smart-retry/main.ts"
