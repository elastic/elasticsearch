#!/bin/bash

set -ue
if [[ $ghprbTargetBranch =~ 6.[0-9x]+ ]]; then
  FORWARD_TARGET=7.x
elif [[ $ghprbTargetBranch =~ 7.[0-9x]+ ]]; then
  FORWARD_TARGET=master
elif [ $ghprbTargetBranch == "master" ]; then
  echo "Already running against master, nothing to do."
  echo "Probably incorrect label."
  exit 0
else
  echo "The target of the PR: $ghprbTargetBranch is not an expected backport target."
  exit 1
fi

echo "Running FWC agaisnt $FORWARD_TARGET"
git checkout $FORWARD_TARGET
.ci/bild.sh  -Dbwc.remote="" -Dbwc.refspec.$ghprbTargetBranch=$GIT_COMMIT bwcTestSnapshots
