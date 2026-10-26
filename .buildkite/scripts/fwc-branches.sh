#!/bin/bash

# Configure FwC test branches
# Forward compatibility tests upgrade the tip of the previous minor to the released patches of a
# branch's own minor, so only run them for branches that:
# - have released at least one minor version (not main)
# - have a previous minor that is still a development branch. For M.0 this is any (M-1).x branch.
# This mirrors how elasticsearch.fwc-test.gradle decides whether to register any fwcTest tasks.
hasPreviousMinorBranch() {
  local major=$1
  local minor=$2
  for candidate in "${BRANCHES[@]}"; do
    if [[ "$minor" -gt 0 && "$candidate" == "$major.$((minor - 1))" ]]; then
      return 0
    fi
    if [[ "$minor" -eq 0 && "$candidate" =~ ^$((major - 1))\.[0-9]+$ ]]; then
      return 0
    fi
  done
  return 1
}

FWC_BRANCHES=()
for branch in "${BRANCHES[@]}"; do
  if [[ "$branch" =~ ^([0-9]+)\.([0-9]+)$ ]] && hasPreviousMinorBranch "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"; then
    FWC_BRANCHES+=("$branch")
  fi
done

shouldRunFwcFor() {
  local branch=$1
  for fwc_branch in "${FWC_BRANCHES[@]}"; do
    if [[ "$fwc_branch" == "$branch" ]]; then
      return 0
    fi
  done
  return 1
}
