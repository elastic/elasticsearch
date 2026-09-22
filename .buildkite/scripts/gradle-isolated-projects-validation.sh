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
    exit "$gradle_exit"
  fi
  exit 1
fi

tmp_count_file=$(mktemp)
tmp_summary_file=$(mktemp)
trap 'rm -f "$tmp_count_file" "$tmp_summary_file"' EXIT

python3 - "$REPORT_FILE" "$tmp_count_file" "$tmp_summary_file" <<'PY'
import json
import sys
from pathlib import Path

report_file = Path(sys.argv[1])
count_file = Path(sys.argv[2])
summary_file = Path(sys.argv[3])

report = json.loads(report_file.read_text())
count_file.write_text(str(report.get("totalProblems", 0)))

lines = ["Severity breakdown:"]
for severity in report.get("severities", []):
    lines.append(f"- {severity['severity']}: {severity['count']}")

lines.append("")
lines.append("Top 10 problem IDs:")
for problem in report.get("problems", [])[:10]:
    lines.append(f"- {problem['count']}x {problem['id']} ({problem['severity']})")

summary_file.write_text("\n".join(lines))
PY

violation_count=$(<"$tmp_count_file")
summary=$(<"$tmp_summary_file")

annotation_style="info"
if (( gradle_exit != 0 || violation_count > MAX_ISOLATED_PROJECTS_VIOLATIONS )); then
  annotation_style="error"
fi

if command -v buildkite-agent >/dev/null 2>&1; then
  cat <<EOF | buildkite-agent annotate --context "$ANNOTATION_CONTEXT" --style "$annotation_style"
### Gradle isolated projects validation

- Gradle exit code: `$gradle_exit`
- Violations: `$violation_count`
- Threshold: `$MAX_ISOLATED_PROJECTS_VIOLATIONS`
- Report: `$REPORT_FILE`

$summary
EOF
fi

echo "Gradle exit code: $gradle_exit"
echo "Isolated projects violations: $violation_count"
echo "Allowed threshold: $MAX_ISOLATED_PROJECTS_VIOLATIONS"
printf '%s\n' "$summary"

if (( gradle_exit != 0 )); then
  echo "Gradle command failed"
  exit "$gradle_exit"
fi

if (( violation_count > MAX_ISOLATED_PROJECTS_VIOLATIONS )); then
  echo "Isolated projects violations exceed threshold"
  exit 1
fi

echo "Isolated projects violations are within threshold"
