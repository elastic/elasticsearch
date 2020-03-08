#!/usr/bin/env bash
set -e

if [ "$#" -eq 0 ]; then
    printf 'Usage: %s <origin> <branch> <branch> ...\n' "$(basename "$0")"
    exit 0;
fi

REMOTE="$1"

if ! git ls-remote --exit-code "${REMOTE}" > /dev/null 2>&1; then
    echo >&2 "Remote '${REMOTE}' is not valid."
    exit 1;
fi

if [ "$#" -lt 3 ]; then
    echo >&2 "You must specify at least two branchs to push."
    exit 1;
fi

if ! git diff-index --quiet HEAD -- ; then
    echo >&2 "Found uncommitted changes in working copy."
    exit 1;
fi

ATOMIC_COMMIT_DATE="$(date)"

for BRANCH in "${@:2}"
do
    echo "Validating branch '${BRANCH}'..."
    
    # Vailidate that script arguments are valid local branch names
    if ! git show-ref --verify --quiet "refs/heads/${BRANCH}"; then
        echo >&2 "No such branch named '${BRANCH}'."
        exit 1;
    fi
    
    # Pull and rebase all branches to ensure we've incorporated any new upstream commits
    git checkout --quiet ${BRANCH}
    git pull "${REMOTE}" --rebase --quiet
    
    PENDING_COMMITS=$(git log ${REMOTE}/${BRANCH}..HEAD --oneline | grep "^.*$" -c || true)
    
    # Ensure that there is exactly 1 unpushed commit in the branch
    if [ "${PENDING_COMMITS}" -ne 1 ]; then
        echo >&2 "Expected exactly 1 pending commit for branch '${BRANCH}' but ${PENDING_COMMITS} exist."
        exit 1;
    fi
    
    # Amend HEAD commit to ensure all branch commit dates are the same
    GIT_COMMITTER_DATE="${ATOMIC_COMMIT_DATE}" git commit --amend --no-edit --quiet
done

echo "Pushing to remote '${REMOTE}'..."
git push --atomic "${REMOTE}" "${@:2}"
