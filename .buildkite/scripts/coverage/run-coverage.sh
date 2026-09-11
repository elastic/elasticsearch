#!/bin/bash
#
# Measure code coverage for a chosen slice of the build, per layer and merged.
#
#   .buildkite/scripts/coverage/run-coverage.sh
#
# Parameters, all optional, all environment variables:
#
#   COVERAGE_PROJECTS  Gradle project-path pattern (default ':x-pack:plugin:esql-datasource-*')
#   COVERAGE_INCLUDES  JaCoCo class-include pattern, colon-separated
#   COVERAGE_LAYERS    comma-separated: unit,internal-cluster,cluster  (default: all three)
#   COVERAGE_OUTPUT    output directory (default: build/coverage)
#   COVERAGE_PORT      collector port for cluster-node coverage (default: 6300)
#
# Produces, under COVERAGE_OUTPUT:
#   exec/<layer>/  raw execution data, one file per task, kept per layer
#   report/<layer> HTML + XML for each layer on its own
#   report/merged  HTML + XML for the union, when more than one layer ran
#   summary.txt    headline numbers
#
# Layers are reported separately AND merged. They are never averaged: a line covered by two layers
# is one covered line, so the union has to be computed from the execution data itself. Merging is
# what `jacococli merge` is for.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"
# shellcheck source=lib.sh
source "$ROOT/.buildkite/scripts/coverage/lib.sh"

PROJECTS="${COVERAGE_PROJECTS:-:x-pack:plugin:esql-datasource-*}"
INCLUDES="${COVERAGE_INCLUDES:-org.elasticsearch.xpack.esql.*:org.elasticsearch.compute.*:org.elasticsearch.arrow.*}"
LAYERS="${COVERAGE_LAYERS:-unit,internal-cluster,cluster}"
OUT="${COVERAGE_OUTPUT:-$ROOT/build/coverage}"
PORT="${COVERAGE_PORT:-6300}"

# In CI, hooks/pre-command exports GRADLEW with the build cache and scan flags; locally the plain
# wrapper is fine. Unquoted expansion below is deliberate: GRADLEW carries its flags inline.
GRADLE="${GRADLEW:-./gradlew}"

LIB="$OUT/lib"
EXEC="$OUT/exec"
REPORT="$OUT/report"
mkdir -p "$LIB" "$EXEC" "$REPORT"

# --- tooling -------------------------------------------------------------------------------------

coverage_fetch_tools "$LIB"

COLLECTOR_SRC="$ROOT/.buildkite/scripts/coverage/CollectorServer.java"
if [[ ! -f "$LIB/CollectorServer.class" || "$COLLECTOR_SRC" -nt "$LIB/CollectorServer.class" ]]; then
  javac -cp "$CLI" -d "$LIB" "$COLLECTOR_SRC"
fi

# --- which tasks exist ---------------------------------------------------------------------------
#
# Enumerated (see lib.sh) rather than hardcoded, so a module that gains a suite is picked up
# automatically instead of being silently missed.

# --- run each layer ------------------------------------------------------------------------------

COLLECTOR_PID=""
start_collector() {
  local dest="$1"
  java -cp "$LIB:$CLI" CollectorServer "$PORT" "$dest" &
  COLLECTOR_PID=$!
  sleep 2
  if ! kill -0 "$COLLECTOR_PID" 2>/dev/null; then
    echo "collector failed to start (port $PORT in use?)" >&2
    exit 1
  fi
}
stop_collector() {
  if [[ -n "$COLLECTOR_PID" ]]; then
    kill "$COLLECTOR_PID" 2>/dev/null || true
    wait "$COLLECTOR_PID" 2>/dev/null || true
  fi
  COLLECTOR_PID=""
}
# No orphaned collector if the script dies mid-layer (e.g. a step timeout kills gradle).
trap stop_collector EXIT

# First non-zero gradle exit code across layers. Reported at the very end - after exec collection,
# the zero gate and the reports - so a leg with failing tests still yields its data and then shows
# honestly red. Coverage from the tasks that did run is a valid lower bound.
WORST_RC=0

run_layer() {
  local layer="$1"; shift
  local tasks="$*"
  if [[ -z "${tasks// /}" ]]; then
    echo "--- $layer: no tasks, skipping"
    return 0
  fi

  echo "--- running $layer: $tasks"
  local layer_exec="$EXEC/$layer"
  mkdir -p "$layer_exec"

  # Cluster layers need the collector: node processes dial out to it, because they are killed
  # rather than shut down and cannot write a file on exit. coverage.port is only passed for this
  # layer — it is what arms the node-instrumentation channel in gradle/coverage.gradle, so unit
  # and internal-cluster test JVMs never carry a tcpclient agent pointing at a dead port.
  local port_arg=()
  if [[ "$layer" == "cluster" ]]; then
    start_collector "$layer_exec/nodes.exec"
    port_arg=("-Dcoverage.port=$PORT")
  fi

  set +e
  # shellcheck disable=SC2086
  $GRADLE --continue \
    -I gradle/coverage.gradle \
    -Dcoverage.agent="$AGENT" \
    -Dcoverage.includes="$INCLUDES" \
    -Dcoverage.output="$OUT/$layer" \
    -Dcoverage.projects="$PROJECTS" \
    ${port_arg[@]+"${port_arg[@]}"} \
    $tasks
  local rc=$?
  set -e

  [[ "$layer" == "cluster" ]] && stop_collector

  find "$OUT/$layer/exec" -name '*.exec' -exec mv {} "$layer_exec/" \; 2>/dev/null || true
  echo "--- $layer finished (gradle rc=$rc)"
  [[ "$rc" -ne 0 && "$WORST_RC" -eq 0 ]] && WORST_RC=$rc
  return 0
}

# Every test task on the matched projects must map to a layer. A suite we do not know how to
# measure is a loud failure, not a silent omission - otherwise we report a smaller number as though
# it were the whole picture.
# Ask Gradle for the real test tasks - never guess them from directory names.
TASKLIST="$OUT/tasks.list"
echo "--- enumerating test tasks from Gradle"
coverage_enumerate_tasks "$ROOT" "$PROJECTS" "$TASKLIST" "$GRADLE"
echo "--- $(wc -l < "$TASKLIST" | tr -d ' ') test tasks found"

IFS=',' read -ra WANTED <<< "$LAYERS"
for layer in "${WANTED[@]}"; do
  case "$layer" in
    unit|internal-cluster|cluster) ;;
    *) echo "unknown layer '$layer' in COVERAGE_LAYERS (expected unit,internal-cluster,cluster)" >&2; exit 1 ;;
  esac
done
# The enumeration result is captured in an assignment, not substituted inline in the run_layer
# call: a command substitution in an argument position discards its exit status, which would
# reduce the unmapped-task failure to a stderr message while the run continued without the
# unmapped suite. Any unmapped task aborts here, before any Gradle work is spent.
for layer in "${WANTED[@]}"; do
  if ! layer_tasks=$(coverage_tasks_for_layer "$TASKLIST" "$layer"); then
    exit 1
  fi
  run_layer "$layer" "$(printf '%s' "$layer_tasks" | tr '\n' ' ')"
done

# A layer with no matching tasks produces no execution data at all. That is "nothing to measure",
# which is different from the broken-instrument case (tasks ran, nothing recorded) gated below.
if [[ -z "$(find "$EXEC" -name '*.exec' 2>/dev/null | head -1)" ]]; then
  echo "--- no matching tasks ran for $PROJECTS in layers [$LAYERS]; nothing to report"
  exit 0
fi

# --- fail loudly on zero -------------------------------------------------------------------------
#
# An exec file that exists but records nothing is worse than no file: it reads as a finding rather
# than as a broken instrument. This has been mistaken for a result before.

"$ROOT/.buildkite/scripts/coverage/check-nonzero.sh" "$EXEC" "$CLI"

# --- report --------------------------------------------------------------------------------------

PATH_ARGS=()
while IFS= read -r a; do PATH_ARGS+=("$a"); done < <(coverage_report_path_args "$ROOT" "$PROJECTS" "$INCLUDES")
if [[ ${#PATH_ARGS[@]} -eq 0 ]]; then
  echo "no compiled classes found for pattern $PROJECTS - cannot build a report" >&2
  exit 1
fi

report_one() {
  local name="$1"; shift
  [[ $# -eq 0 ]] && return 0
  mkdir -p "$REPORT/$name"
  java -jar "$CLI" report "$@" \
    "${PATH_ARGS[@]}" \
    --html "$REPORT/$name/html" \
    --xml "$REPORT/$name/coverage.xml" \
    --csv "$REPORT/$name/coverage.csv" \
    --name "$name" >/dev/null
}

for layer in "${WANTED[@]}"; do
  execs=()
  while IFS= read -r f; do execs+=("$f"); done < <(find "$EXEC/$layer" -name '*.exec' 2>/dev/null)
  report_one "$layer" ${execs[@]+"${execs[@]}"}
done

# The merged view. Computed from the union of execution data, never from the per-layer
# percentages. Written outside exec/ so the CI artifact glob only ever picks up raw layer data.
if [[ ${#WANTED[@]} -gt 1 ]]; then
  all_execs=()
  while IFS= read -r f; do all_execs+=("$f"); done < <(find "$EXEC" -name '*.exec' 2>/dev/null)
  if [[ ${#all_execs[@]} -gt 0 ]]; then
    java -jar "$CLI" merge "${all_execs[@]}" --destfile "$OUT/merged.exec" >/dev/null
    report_one merged "$OUT/merged.exec"
  fi
fi

"$ROOT/.buildkite/scripts/coverage/summarise.sh" "$REPORT" | tee "$OUT/summary.txt"
echo "--- reports in $REPORT"

if [[ "$WORST_RC" -ne 0 ]]; then
  echo "--- test failures occurred (gradle rc=$WORST_RC); coverage above is a lower bound"
  exit "$WORST_RC"
fi
