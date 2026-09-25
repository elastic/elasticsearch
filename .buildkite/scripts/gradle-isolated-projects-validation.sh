#!/bin/bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
readonly WORKSPACE_DIR="${WORKSPACE:-$REPO_ROOT}"

# Lower this baseline as isolated-projects issues are fixed. The target is zero.
# Locally measured 2950 violations; the ceiling adds headroom for CI-specific
# setups (extra init scripts, agents, environment) that can surface a few more.
readonly DEFAULT_MAX_ISOLATED_PROJECTS_VIOLATIONS=3000
readonly MAX_ISOLATED_PROJECTS_VIOLATIONS="${GRADLE_ISOLATED_PROJECTS_MAX_VIOLATIONS:-$DEFAULT_MAX_ISOLATED_PROJECTS_VIOLATIONS}"
readonly REPORT_ARTIFACT_PATH="build/problems-status.json"
readonly REPORT_FILE="$WORKSPACE_DIR/$REPORT_ARTIFACT_PATH"
readonly ANNOTATION_CONTEXT="ctx-gradle-isolated-projects-validation"
readonly REPORT_ARTIFACT_LINK="<a href=\"artifact://$REPORT_ARTIFACT_PATH\">$REPORT_ARTIFACT_PATH</a>"

annotate() {
  local style="$1"
  local annotation="$2"
  if command -v buildkite-agent >/dev/null 2>&1; then
    printf '%s\n' "$annotation" | buildkite-agent annotate --context "$ANNOTATION_CONTEXT" --style "$style"
  fi
}

upload_report_artifact() {
  if command -v buildkite-agent >/dev/null 2>&1; then
    (
      cd "$WORKSPACE_DIR"
      buildkite-agent artifact upload "$REPORT_ARTIFACT_PATH"
    ) || echo "Failed to upload problems report artifact: $REPORT_FILE"
  fi
}

rm -f "$REPORT_FILE"

set +e
"$REPO_ROOT/.ci/scripts/run-gradle.sh" --isolated-projects -Dorg.gradle.isolated-projects.diagnostics=true :server:precommit
gradle_exit=$?
set -e

if [[ ! -f "$REPORT_FILE" ]]; then
  echo "Expected Gradle problems report at $REPORT_FILE, but it was not created"
  annotation=$(cat <<EOF
### Gradle isolated projects validation

- Gradle exit code: $gradle_exit
- Violations: unavailable
- Threshold: $MAX_ISOLATED_PROJECTS_VIOLATIONS
- Report: $REPORT_FILE

Problems report was not produced, so isolated-projects validation did not complete.
EOF
)
  annotate error "$annotation"
  if (( gradle_exit != 0 )); then
    echo "Gradle command failed before a problems report was produced"
    exit "$gradle_exit"
  fi
  exit 1
fi

upload_report_artifact

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
if (( violation_count > MAX_ISOLATED_PROJECTS_VIOLATIONS || gradle_exit != 0 )); then
  annotation_style="error"
fi

annotation=$(cat <<EOF
### Gradle isolated projects validation

- Gradle exit code: $gradle_exit
- Violations: $violation_count
- Threshold: $MAX_ISOLATED_PROJECTS_VIOLATIONS
- Report: $REPORT_FILE
- Artifact: $REPORT_ARTIFACT_LINK

$summary
EOF
)
annotate "$annotation_style" "$annotation"

echo "Gradle exit code: $gradle_exit"
echo "Isolated projects validation violations: $violation_count"
echo "Allowed threshold: $MAX_ISOLATED_PROJECTS_VIOLATIONS"
echo "Problems report: $REPORT_FILE"
if command -v buildkite-agent >/dev/null 2>&1; then
  echo "Problems report artifact: $REPORT_ARTIFACT_PATH"
fi
printf '%s\n' "$summary"

if (( gradle_exit != 0 )); then
  echo "Gradle command failed; isolated-projects validation requires a successful build so the violation count is trustworthy"
  exit "$gradle_exit"
fi

if (( violation_count > MAX_ISOLATED_PROJECTS_VIOLATIONS )); then
  echo "Isolated projects violations exceed threshold"
  exit 1
fi

echo "Isolated projects violations are within threshold"
