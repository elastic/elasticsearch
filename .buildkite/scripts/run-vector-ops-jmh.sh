#!/bin/bash

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

set -euo pipefail

merge_base=$(git merge-base "${GITHUB_PR_TARGET_BRANCH}" HEAD)
pr_commit = "${GITHUB_PR_TRIGGERED_SHA}"

# Build, run and store JMH benchmarks for the merge base
echo "Building and running JMH benchmarks for the merge base: ${merge_base}"
git checkout "$merge_base"

# TODO Run all of VectorScorer*
.ci/scripts/run-gradle.sh :benchmarks:run --args 'VectorScorerInt7Benchmark -rf text -rff build/result-baseline.txt'

# Store the results of the merge base benchmarks
buildkite-agent artifact upload "build/result-baseline.txt"
echo "Stored JMH benchmark results for the merge base: ${merge_base}"

# Build, run and store JMH benchmarks for the PR commit
echo "Building and running JMH benchmarks for the PR commit: ${pr_commit}"
git checkout "$pr_commit"
# but first build the native vector code and set the environment variable indicating we should use it
LOCAL_VEC_BINARY_OS="linux"
cd libs/simdvec/native
./gradlew buildSharedLibraryAndCopy
# go back up to the repo root
cd ../../../..
# run the benchmarks with the native vector code
# TODO Run all of VectorScorer*
.ci/scripts/run-gradle.sh :benchmarks:run --args 'VectorScorerInt7Benchmark -rf text -rff build/result-candidate.txt'

# Store the results of the PR commit benchmarks
buildkite-agent artifact upload "build/result-candidate.txt"
echo "Stored JMH benchmark results for the PR commit: ${pr_commit}"
