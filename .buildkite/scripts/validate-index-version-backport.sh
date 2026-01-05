#!/bin/bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

set -euo pipefail
BUILDKITE_PULL_REQUEST_BASE_BRANCH="test"
if [[ "${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" == "main" ]]; then
  # Don't run on PRs targeting main
  exit 0
fi

echo "--- Looking for index version changes"

# Get any changes in this pull request to transport definitions
git fetch origin "${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" --quiet
changed_file=$(git diff --name-only "origin/${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" | grep -E "server/src/main/java/org/elasticsearch/index/IndexVersions.java" || true)

if [[ -z "${changed_file}" ]]; then
  echo "No index version changes detected."
  exit 0
fi

echo "${change_file}"

# Compare those files against the main branch to ensure they are the same
#git fetch origin main --quiet
#while IFS= read -r file; do
#  if ! git diff --quiet origin/main -- "${file}"; then
#      echo "Changes to index definition [${file}] missing from main branch."
#      echo "Transport changes must first be merged to main before being backported."
#      exit 1
#  fi
#done <<< "${changed_file}"

echo "All transport changes exist in main branch."
