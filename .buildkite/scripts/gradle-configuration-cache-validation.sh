#!/bin/bash
#
# Validates that the configuration cache entry for the given Gradle tasks is reused.
# The tasks are run three times:
#   1. without the configuration cache, to warm up (workaround for
#      https://github.com/gradle/gradle/issues/28159, precompiled script plugins
#      require an additional run before their configuration cache entry is reusable)
#   2. with the configuration cache, which stores the entry
#   3. with the configuration cache again, which must reuse the stored entry
#
# A failure of the last run means some build logic reads state that invalidates the
# configuration cache (e.g. an undeclared file, environment variable or system
# property). The Gradle output of that run names the reason.

set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 <gradle task>..."
  exit 1
fi

readonly CONFIGURATION_CACHE_ARGS=(
  --configuration-cache
  "-Dorg.gradle.configuration-cache.inputs.unsafe.ignore.file-system-checks=build/*.tar.bz2"
)

# Note: run-gradle.sh launches Gradle through the Tooling API, which rejects daemon
# options such as --no-daemon.
echo "--- Warm-up run without configuration cache: $*"
.ci/scripts/run-gradle.sh "$@"

echo "--- First run with configuration cache (stores the entry): $*"
.ci/scripts/run-gradle.sh "${CONFIGURATION_CACHE_ARGS[@]}" "$@"

tmpOutputFile=$(mktemp)
trap 'rm -f "$tmpOutputFile"' EXIT

echo "--- Second run with configuration cache (must reuse the entry): $*"
.ci/scripts/run-gradle.sh "${CONFIGURATION_CACHE_ARGS[@]}" "$@" | tee "$tmpOutputFile"

if grep -q "Configuration cache entry reused." "$tmpOutputFile"; then
  echo "Gradle configuration cache reused"
  exit 0
fi

echo "+++ Failed to reuse Gradle configuration cache for: $*"
reason=$(grep -m1 -A5 "Calculating task graph as configuration cache cannot be reused" "$tmpOutputFile" || true)
if [[ -n "$reason" ]]; then
  echo "$reason"
fi
if command -v buildkite-agent > /dev/null 2>&1; then
  {
    echo "**Configuration cache entry for \`$*\` was not reused** on the second run."
    echo
    echo "Some build logic reads state that invalidates the configuration cache (e.g. an undeclared file, environment variable or system property)."
    if [[ -n "$reason" ]]; then
      echo
      echo '```'
      echo "$reason"
      echo '```'
    else
      echo "Search the job log for \`configuration cache cannot be reused\` to find the reason."
    fi
  } | buildkite-agent annotate --style error --context "ctx-configuration-cache-${*// /-}" || true
fi
exit 1
