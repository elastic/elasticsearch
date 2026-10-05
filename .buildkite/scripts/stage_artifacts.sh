#!/usr/bin/env bash
set -euo pipefail

WORKFLOW="${DRA_WORKFLOW:?DRA_WORKFLOW is required}"

echo "--- :compression: Downloading ${WORKFLOW} artifacts"

buildkite-agent artifact download 'artifacts/*' .

if ls artifacts/* 1>/dev/null 2>&1; then
  chmod -R a+r artifacts/
fi

echo "--- :package: Verifying staged ${WORKFLOW} artifacts"

if ! ls artifacts/* 1>/dev/null 2>&1; then
  echo "ERROR: no ${WORKFLOW} artifacts found in artifacts/ after download." >&2
  exit 1
fi

echo "Staged artifacts:"
ls -1 artifacts/
