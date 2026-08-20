#!/bin/bash
set -euo pipefail

# Builds libvec from source "transitively", by running the simdvec tests against it; when running with
# VEC_NATIVE_BUILD=docker, the gradle build will compile libvec for all platforms, inside the cross-compilation
# toolchain image, and use it for all the following tasks. Skips when the pull request touches no native sources:
# the pipeline config cannot express "run when any of these files changed", so the check lives here.

echo "--- Looking for native vec changes"

git fetch origin "${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" --quiet
changed_files=$(git diff --name-only "origin/${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" | grep -E \
  "^(libs/simdvec/native/|build-tools-internal/src/main/java/org/elasticsearch/gradle/internal/nativelibs/|libs/native/libraries/build\.gradle$)" || true)

if [[ -z "${changed_files}" ]]; then
  echo "No native vec changes detected, skipping build from source."
  exit 0
fi

echo "Native vec changes detected:"
echo "${changed_files}"

echo "--- Building libvec from source and running simdvec tests"
.ci/scripts/run-gradle.sh :libs:simdvec:check
