#!/bin/bash
set -euo pipefail

if [[ "${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" == "main" ]]; then
  # Don't run on PRs targeting main
  exit 0
fi

echo "--- Looking for transport version changes"

# Get any changes in this pull request to transport definitions
git fetch origin "${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" --quiet
changed_files=$(git diff --name-only "origin/${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" | grep -E "server/src/main/resources/transport/definitions/.*\.csv" || true)

if [[ -z "${changed_files}" ]]; then
  echo "No transport version changes detected."
  exit 0
fi

# Compare those files against all the higher branches to ensure they are the same
higher_branches=$(jq -r '.branches[].branch' branches.json | while read -r branch; do
  if [[ "${branch}" == "main" ]]; then
    echo "${branch}"
  elif [[ "${branch}" != "${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" ]] &&
    printf '%s\n%s\n' "${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" "${branch}" | sort -VC; then
    echo "${branch}"
  fi
done)

for br in ${higher_branches}; do
  git fetch origin "${br}" --quiet
  while IFS= read -r file; do
    if ! git diff --quiet "origin/${br}" -- "${file}"; then
      echo "Changes to transport definition [${file}] missing from higher branch [${br}]."
      echo "Backports must first be merged on all higher release branches; merge to [${br}] before [${BUILDKITE_PULL_REQUEST_BASE_BRANCH}]."
      exit 1
    fi
  done <<< "${changed_files}"
done

echo "All transport changes exist in higher release branches."
