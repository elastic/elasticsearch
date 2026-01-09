#!/bin/bash
set -euo pipefail

if [[ -z "${BUILDKITE_PULL_REQUEST:-}" ]]; then
  echo "Not a pull request, skipping transport version update"
  exit 0
fi

if ! git diff --exit-code; then
  echo "Changes are present before updating transport versions, not running"
  git status
  exit 0
fi

NEW_COMMIT_MESSAGE="[CI] Update transport version definitions"

echo "--- Generating updated transport version definitions"
# Calculate backport branches based on pull request version labels
backport_branches=$(
  echo "${GITHUB_PR_LABELS}" \
    | tr ',' '\n' \
    | grep -E "v[0-9]+\.[0-9]+\.[0-9]+" \
    | sed -E 's/^v([0-9]+)\.([0-9]+)\.[0-9]+$/\1.\2/' \
    | paste -sd, -
)

if [[ -z "${backport_branches}" ]]; then
  echo "Skipping as pull request contains no version labels"
  exit 0
fi

.ci/scripts/run-gradle.sh generateTransportVersion --backport-branches="${backport_branches}"

if git diff --quiet -- "server/src/main/resources/transport/**"; then
  echo "No transport version changes found after update. Skipping auto commit."
  exit 0
fi

git config --global user.name elasticsearchmachine
git config --global user.email 'infra-root+elasticsearchmachine@elastic.co'

gh pr checkout "${BUILDKITE_PULL_REQUEST}"
git add -A .
git commit -m "$NEW_COMMIT_MESSAGE"
git push

# After the git push, the new commit will trigger a new build within a few seconds and this build should get cancelled
# So, let's just sleep to give the build time to cancel itself without an error
# If it doesn't get cancelled for some reason, then exit with an error, because we don't want this build to be green (we just don't want it to generate an error either)
sleep 300
exit 1
