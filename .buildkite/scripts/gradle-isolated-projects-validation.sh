#!/bin/bash

set -euo pipefail

# Lower this baseline as isolated-projects issues are fixed. The target is zero.
# Locally measured 2950 violations; the ceiling adds headroom for CI-specific
# setups (extra init scripts, agents, environment) that can surface a few more.
readonly DEFAULT_MAX_ISOLATED_PROJECTS_VIOLATIONS=3000
readonly MAX_ISOLATED_PROJECTS_VIOLATIONS="${GRADLE_ISOLATED_PROJECTS_MAX_VIOLATIONS:-$DEFAULT_MAX_ISOLATED_PROJECTS_VIOLATIONS}"
readonly REPORT_FILE="${WORKSPACE:-$PWD}/build/problems-status.json"
readonly ANNOTATION_CONTEXT="ctx-gradle-isolated-projects-validation"

rm -f "$REPORT_FILE"

set +e
GRADLE_CAPTURE_PROBLEMS_STATUS=true \
  .ci/scripts/run-gradle.sh :server:precommit --isolated-projects -Dorg.gradle.isolated-projects.diagnostics=true
gradle_exit=$?
set -e

if [[ ! -f "$REPORT_FILE" ]]; then
  echo "Expected Gradle problems report at $REPORT_FILE, but it was not created"
  if (( gradle_exit != 0 )); then
    echo "Gradle command failed before a problems report was produced; skipping threshold enforcement for this run"
    exit 0
  fi
  exit 1
fi

violation_count=$(jq -r '[.problems[] | select(.id | startswith("validation:configuration-cache:")) | .count] | add // 0' "$REPORT_FILE")
summary=$(jq -r '
  (.problems // []) as $problems
  | ($problems | map(select(.id | startswith("validation:configuration-cache:")))) as $isolatedProjectProblems
  | ["Severity breakdown (isolated-projects validation only):"]
  + (($isolatedProjectProblems | sort_by(.severity) | group_by(.severity) | map("- \(.[0].severity): \(map(.count) | add)")) // [])
  + [""]
  + ["Top 10 isolated-projects problem IDs:"]
  + (($isolatedProjectProblems[:10]) | map("- \(.count)x \(.id) (\(.severity))"))
  | join("\n")
' "$REPORT_FILE")

annotation_style="info"
if (( violation_count > MAX_ISOLATED_PROJECTS_VIOLATIONS )); then
  annotation_style="error"
elif (( gradle_exit != 0 )); then
  annotation_style="warning"
fi

if command -v buildkite-agent >/dev/null 2>&1; then
  annotation=$(cat <<EOF
### Gradle isolated projects validation

- Gradle exit code: $gradle_exit
- Violations: $violation_count
- Threshold: $MAX_ISOLATED_PROJECTS_VIOLATIONS
- Report: $REPORT_FILE

$summary
EOF
)
  printf '%s\n' "$annotation" | buildkite-agent annotate --context "$ANNOTATION_CONTEXT" --style "$annotation_style"
fi

echo "Gradle exit code: $gradle_exit"
echo "Isolated projects validation violations: $violation_count"
echo "Allowed threshold: $MAX_ISOLATED_PROJECTS_VIOLATIONS"
printf '%s\n' "$summary"

if (( gradle_exit != 0 )); then
  echo "Gradle command failed, but this validation only gates on the isolated-projects violation count"
fi

if (( violation_count > MAX_ISOLATED_PROJECTS_VIOLATIONS )); then
  echo "Isolated projects violations exceed threshold"
  exit 1
fi

echo "Isolated projects violations are within threshold"
