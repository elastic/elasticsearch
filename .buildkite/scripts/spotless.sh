#!/bin/bash

if [[ -z "${BUILDKITE_PULL_REQUEST:-}" ]]; then
  echo "Not a pull request, skipping spotless"
  exit 0
fi

if ! git diff --exit-code; then
  echo "Changes are present before running spotless, not running"
  git status
  exit 0
fi

NEW_COMMIT_MESSAGE="[CI] Auto commit changes from spotless"
PREVIOUS_COMMIT_MESSAGE="$(git log -1 --pretty=%B)"

echo "--- Running spotless"
.ci/scripts/run-gradle.sh -Dscan.tag.NESTED spotlessApply

if git diff --exit-code; then
  echo "No changes found after running spotless. Don't need to auto commit."
  exit 0
fi

if [[ "$NEW_COMMIT_MESSAGE" == "$PREVIOUS_COMMIT_MESSAGE" ]]; then
  echo "Changes found after running spotless"
  echo "CI already attempted to commit these changes, but the file(s) seem to have changed again."
  echo "Please review and fix manually."
  exit 1
fi

git config --global user.name elasticsearchmachine
git config --global user.email 'infra-root+elasticsearchmachine@elastic.co'

gh pr checkout "${BUILDKITE_PULL_REQUEST}"
git add -u .
git commit -m "$NEW_COMMIT_MESSAGE"
git push

# After the git push, the new commit will trigger a new build within a few seconds and this build should get cancelled
# So, let's just sleep to give the build time to cancel itself without an error
# If it doesn't get cancelled for some reason, then exit with an error, because we don't want this build to be green (we just don't want it to generate an error either)
sleep 300
exit 1
