#!/bin/bash
set -euo pipefail

echo "--- Looking for transport version changes"

# Get any changes in this pull request to transport definitions
git fetch origin "${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" --quiet
changed_files=$(git diff --name-only "origin/${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" | grep -E "server/src/main/resources/transport/definitions/.*\.csv" || true)

if [[ -z "${changed_files}" ]]; then
  echo "No transport version changes detected."
  exit 0
fi

# Compare those files against the main branch to ensure they are the same
git fetch origin main --quiet
while IFS= read -r file; do
  if ! git diff --quiet origin/main -- "${file}"; then
      echo "Changes to transport definition [${file}] missing from main branch."
      echo "Transport changes must first be merged to main before being backported."
      exit 1
  fi
done <<< "${changed_files}"

echo "All transport changes exist in main branch."
