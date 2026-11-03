#!/bin/bash
#
# Check out a branch that is ahead of the PR's target branch and run its backwards compatibility
# tests, but build the old-version snapshot from the PR head instead of from the tip of the target
# branch. See .buildkite/pipelines/pull-request/later-branch-bwc.yml.

set -euo pipefail

PR_SHA="${BUILDKITE_COMMIT:-}"
TARGET_BRANCH="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-${GITHUB_PR_TARGET_BRANCH:-}}"
TARGET_VERSION=$(sed -n 's/^elasticsearch[[:space:]]*=[[:space:]]*//p' build-tools-internal/version.properties)

if [[ -z "$PR_SHA" || -z "$TARGET_BRANCH" || -z "$TARGET_VERSION" ]]; then
  echo "Could not determine the PR commit [$PR_SHA], target branch [$TARGET_BRANCH] and version [$TARGET_VERSION]"
  exit 1
fi

echo "--- Preparing $LATER_BRANCH checkout to test $TARGET_BRANCH ($TARGET_VERSION) at $PR_SHA"

# Pin the PR head to a ref before we leave it. Buildkite fetches the pull request into FETCH_HEAD and
# checks it out detached, so no ref points at the commit, and the bwc checkout is a clone of this
# workspace, which only carries objects that are reachable from a ref.
git branch --force "later-branch-bwc-pr-${BUILDKITE_BUILD_NUMBER}" "$PR_SHA"

git fetch --no-tags --quiet origin "$LATER_BRANCH"
git checkout --force -B "$LATER_BRANCH" "origin/$LATER_BRANCH"

# The workspace is on the later branch now, but buildkite still describes the pull request. Anything
# resolving a base ref from the environment, such as the transport version resources, has to compare
# against the branch we are actually building.
export BUILDKITE_PULL_REQUEST_BASE_BRANCH="$LATER_BRANCH"

# -Dtests.bwc.mode=gradle is required: under "auto" the build may download a prebuilt DRA snapshot,
# whose lookup keys off elastic/<branch> rather than the refspec below, and we would end up testing
# the tip of the target branch while appearing to pass.
.ci/scripts/run-gradle.sh \
  -Dignore.tests.seed \
  -Dtests.bwc.mode=gradle \
  -Dtests.bwc.git_fetch_latest=false \
  "-Dbwc.refspec.${TARGET_BRANCH}=${PR_SHA}" \
  "v${TARGET_VERSION}#bwcTestPart${BWC_PART}"
