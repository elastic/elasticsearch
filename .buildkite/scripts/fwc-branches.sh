#!/bin/bash

# Configure FwC test branches
# We do not want 7.x branch and only to run for branches that:
# - have released at least one minor version (not main)
# - have previous minor unreleased (not the oldest development branch)
FWC_BRANCHES=()
for branch in "${BRANCHES[@]}"; do
  if [[ ! "$branch" =~ ^7\..* ]]; then
    FWC_BRANCHES+=("$branch")
  fi
done
# Remove first and last element
FWC_BRANCHES=("${FWC_BRANCHES[@]:1:${#FWC_BRANCHES[@]}-2}")

shouldRunFwcFor() {
  local branch=$1
  for fwc_branch in "${FWC_BRANCHES[@]}"; do
    if [[ "$fwc_branch" == "$branch" ]]; then
      return 0
    fi
  done
  return 1
}
