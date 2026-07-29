# shellcheck shell=bash
#
# Shared helpers for the coverage scripts. Sourced, not executed.
#
# Everything here is parameterised by the Gradle project-path pattern; nothing names a specific
# module. The scope is an input, the mechanism is fixed.

JACOCO_VERSION=0.8.12

# Fetch the JaCoCo agent + CLI into $1 and set AGENT / CLI.
coverage_fetch_tools() {
  local lib="$1"
  local base="https://repo1.maven.org/maven2/org/jacoco"
  AGENT="$lib/org.jacoco.agent-$JACOCO_VERSION-runtime.jar"
  CLI="$lib/org.jacoco.cli-$JACOCO_VERSION-nodeps.jar"
  mkdir -p "$lib"
  if [[ ! -f "$AGENT" ]]; then
    echo "--- fetching JaCoCo $JACOCO_VERSION"
    curl -sSfL "$base/org.jacoco.agent/$JACOCO_VERSION/org.jacoco.agent-$JACOCO_VERSION-runtime.jar" -o "$AGENT"
  fi
  if [[ ! -f "$CLI" ]]; then
    curl -sSfL "$base/org.jacoco.cli/$JACOCO_VERSION/org.jacoco.cli-$JACOCO_VERSION-nodeps.jar" -o "$CLI"
  fi
}

# Translate a Gradle project-path pattern (':x-pack:plugin:esql-datasource-*') into an anchored
# extended regex. '*' matches any run of characters, including ':' — same semantics as the regex
# in gradle/coverage.gradle. Keep the two in sync.
coverage_pattern_regex() {
  local p="$1"
  p="${p//./\\.}"
  p="${p//\*/.*}"
  printf '^%s$' "$p"
}

# Enumerate Gradle project paths matching a pattern, one per line.
#
# Enumerated from the filesystem, mirroring settings.gradle's own discovery (addSubProjects walks
# directories that contain a build.gradle). This deliberately avoids invoking Gradle: a
# `./gradlew projects` call configures the whole build and costs minutes per call. The
# approximation is one-sided: a directory with a build.gradle that settings.gradle does not
# include would surface as a loud "project not found" from Gradle, never as a silent miss.
# Renamed projects (`:libs:native:native-libraries`, `:test:external-modules:test-*`) will not
# resolve under their renamed path; none are test-bearing today.
coverage_projects() {
  local root="$1" pattern="$2"
  local re
  re=$(coverage_pattern_regex "$pattern")
  (
    cd "$root" && find . -mindepth 2 -name build.gradle \
      -not -path '*/build/*' \
      -not -path '*/src/*' \
      -not -path './buildSrc/*' \
      -not -path './build-tools/*' \
      -not -path './build-tools-internal/*' \
      -not -path './build-conventions/*' \
      -not -path './.git/*' \
      2>/dev/null
  ) | sed -e 's#^\./##' -e 's#/build\.gradle$##' -e 's#/#:#g' -e 's#^#:#' \
    | { grep -E "$re" || true; } \
    | { if [[ -n "$(coverage_exclude_regex)" ]]; then grep -Ev "$(coverage_exclude_regex)" || true; else cat; fi; } \
    | sort
}

# Map a project path to its directory under the repo root.
coverage_project_dir() {
  local root="$1" path="$2"
  printf '%s/%s' "$root" "$(printf '%s' "${path#:}" | tr ':' '/')"
}

# How each Gradle test task maps to a coverage layer.
#
# Task NAMES are never guessed - they come from Gradle itself (coverage_enumerate_tasks below),
# because they do not reliably follow their source set: src/csvSpecTest registers a task called
# `csvSpecTests`, plural. A directory heuristic got this wrong in four separate ways, reporting 5
# csvSpecTests instead of 7, zero yamlRestTest instead of 4, one internalClusterTest instead of 2,
# and missing javaRestTestSecure, perfSmokeTest and bcUpgradeTest entirely.
#
# What CANNOT be derived is the layer: whether a suite runs product code in the test JVM or in a
# separate ES node process is invisible in the task definition, and it decides whether the TCP
# collector is needed. So this mapping is stated knowledge, and anything unmapped fails loudly.
COVERAGE_TASK_LAYERS=(
  "test:unit"
  "internalClusterTest:internal-cluster"
  "javaRestTest:cluster"
  "yamlRestTest:cluster"
  "csvSpecTests:cluster"
  "javaRestTestSecure:cluster"
)

# Tasks we deliberately do not measure, with the reason.
#   bcUpgradeTest  - runs old-version nodes; their coverage cannot map onto current classfiles.
#   perfSmokeTest  - measures speed, not behaviour; counting it inflates coverage with something
#                    nobody would call a test of correctness. Includable explicitly.
#   v<version>#... - the ~464 BWC-versioned variants, same reason as bcUpgradeTest.
COVERAGE_EXCLUDED_TASKS=(
  "bcUpgradeTest"
  "perfSmokeTest"
)

# Projects excluded from the scope, as an extended regex over Gradle project paths. Each
# alternative is anchored on ':' / end-of-path so it can only ever match a whole path segment —
# a bare substring here would silently swallow any project whose name happens to contain it.
# parquet-rs is being retired, so measuring it drags the aggregate down for code nobody will
# maintain. Override with COVERAGE_EXCLUDE_PROJECTS; set it to the empty string to exclude
# nothing (which is how you measure parquet-rs itself).
COVERAGE_EXCLUDE_PROJECTS_DEFAULT=':esql-datasource-parquet-rs(:|$)|:libs:parquet-rs(:|$)'

# The effective exclusion regex. The `-` (not `:-`) expansion is deliberate: an explicitly empty
# COVERAGE_EXCLUDE_PROJECTS means "exclude nothing", only an unset one means "use the default".
coverage_exclude_regex() {
  printf '%s' "${COVERAGE_EXCLUDE_PROJECTS-$COVERAGE_EXCLUDE_PROJECTS_DEFAULT}"
}

# Ask Gradle for the real test tasks in one configuration pass. Writes a cached list, one task
# path per line. The cache is keyed on the pattern via a sidecar file: a list produced for a
# different COVERAGE_PROJECTS must never be silently reused.
coverage_enumerate_tasks() {
  local root="$1" pattern="$2" out="$3" gradle="${4:-./gradlew}"
  if [[ -s "$out" && -f "$out.pattern" && "$(cat "$out.pattern")" == "$pattern" ]]; then
    return 0
  fi
  rm -f "$out" "$out.pattern"
  ( cd "$root" && $gradle -q -I gradle/coverage-tasks.gradle \
      -Dcoverage.projects="$pattern" -Dcoverage.tasklist="$out" coverageTaskList >/dev/null )
  [[ -f "$out" ]] || { echo "task enumeration produced no list at $out" >&2; return 1; }
  printf '%s' "$pattern" > "$out.pattern"
}

# Emit the task paths for one layer, from the enumerated list.
coverage_tasks_for_layer() {
  local tasklist="$1" layer="$2"
  local exclude
  exclude=$(coverage_exclude_regex)
  local taskpath name entry unmapped=0
  while IFS= read -r taskpath; do
    [[ -n "$taskpath" ]] || continue
    name="${taskpath##*:}"

    # Excluded projects (see COVERAGE_EXCLUDE_PROJECTS_DEFAULT).
    if [[ -n "$exclude" ]] && printf '%s' "$taskpath" | grep -qE "$exclude"; then
      continue
    fi

    # BWC-versioned variants carry a '#' - excluded wholesale.
    [[ "$name" == *"#"* ]] && continue

    local skip=0
    for entry in "${COVERAGE_EXCLUDED_TASKS[@]}"; do
      [[ "$name" == "$entry" ]] && skip=1 && break
    done
    [[ "$skip" -eq 1 ]] && continue

    local matched=0
    for entry in "${COVERAGE_TASK_LAYERS[@]}"; do
      if [[ "$name" == "${entry%%:*}" ]]; then
        matched=1
        [[ "${entry##*:}" == "$layer" ]] && echo "$taskpath"
        break
      fi
    done
    if [[ "$matched" -eq 0 ]]; then
      echo "UNMAPPED TEST TASK: $taskpath" >&2
      unmapped=1
    fi
  done < "$tasklist"

  if [[ "$unmapped" -eq 1 ]]; then
    echo "" >&2
    echo "Classify it in COVERAGE_TASK_LAYERS in lib.sh, or exclude it in COVERAGE_EXCLUDED_TASKS" >&2
    echo "with a reason. Measuring coverage while silently skipping a suite understates the" >&2
    echo "result and reads as a finding." >&2
    return 1
  fi
}

# Emit `--classfiles <dir>` / `--sourcefiles <dir>` argument pairs for every matched project that
# has compiled main classes, one argument per line (read into an array with `while read`).
#
# Classfiles are restricted to the package prefixes of the includes filter ($3, the same
# colon-separated JaCoCo pattern the agent gets). The agent only ever records classes matching
# that filter, so any class outside it would sit in the report as a permanent 0% - vendored
# sources are the concrete case: esql-datasource-orc compiles org.apache.* shims into its main
# classes, and counting them understates the module without measuring anything. Each pattern is
# cut at its first '*' and mapped to the corresponding package directory; nested prefixes are
# deduplicated so no class is analysed twice.
coverage_report_path_args() {
  local root="$1" pattern="$2" includes="${3:-*}"
  local p d main inc prefix dir last
  while IFS= read -r p; do
    [[ -n "$p" ]] || continue
    d=$(coverage_project_dir "$root" "$p")
    main="$d/build/classes/java/main"
    [[ -d "$main" ]] || continue
    local candidates=() emitted=0
    local IFS_pats
    IFS=':' read -ra IFS_pats <<< "$includes"
    for inc in "${IFS_pats[@]}"; do
      prefix="${inc%%\**}"
      prefix="${prefix%.}"
      dir="$main/$(printf '%s' "$prefix" | tr '.' '/')"
      dir="${dir%/}"
      [[ -d "$dir" ]] && candidates+=("$dir")
    done
    last=""
    while IFS= read -r dir; do
      [[ -n "$dir" ]] || continue
      # Sorted order puts an ancestor before its descendants; skip anything already covered.
      if [[ -n "$last" && "$dir/" == "$last/"* ]]; then
        continue
      fi
      echo "--classfiles"
      echo "$dir"
      last="$dir"
      emitted=1
    done < <(printf '%s\n' ${candidates[@]+"${candidates[@]}"} | sort -u)
    if [[ "$emitted" -eq 1 && -d "$d/src/main/java" ]]; then
      echo "--sourcefiles"
      echo "$d/src/main/java"
    fi
  done < <(coverage_projects "$root" "$pattern")
}

# Total executed probes recorded in an exec file, read with the JaCoCo CLI itself — never inferred
# from file size. execinfo rows look like `<16-hex id>  <hits> of <probes>  <class>`; summing the
# hits column distinguishes "sessions but nothing executed" (0) from real data (>0).
coverage_exec_hits() {
  local cli="$1" exec_file="$2"
  java -jar "$cli" execinfo "$exec_file" 2>/dev/null \
    | awk '$3 == "of" && $2 ~ /^[0-9]+$/ { s += $2 } END { print s + 0 }'
}

# Number of agent sessions recorded in an exec file.
coverage_exec_sessions() {
  local cli="$1" exec_file="$2"
  java -jar "$cli" execinfo "$exec_file" 2>/dev/null | { grep -c '^Session ' || true; }
}
