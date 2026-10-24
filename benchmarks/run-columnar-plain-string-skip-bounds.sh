#!/usr/bin/env bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#
# Runner for the plain string range and terms slicing benchmarks, with the plain skip index off and on.
# Options left unset keep the defaults declared on the benchmark class.
# Works with bash 3.2 (macOS default).

set -euo pipefail

# ---- CONFIG -------------------------------------------------------------------------------------
BENCHMARK="ColumnarPlainString"   # class name, simple or fully qualified
RUNNER="es"                           # es: ./gradlew -p benchmarks run --args | lucene: java --module-path
GRADLE_PROJECT_DIR="benchmarks"       # es only
LUCENE_MODULE_PATH="lucene/benchmark-jmh/build/benchmarks"   # lucene only, created by :lucene:benchmark-jmh:assemble
DEFAULT_OUT_DIR="build/bench-results"
# One entry per @Param on the benchmark class: "cli-option:jmhParamName"
PARAM_SPECS=("format:format" "data:data" "num-docs:numDocs" "selectivity:selectivity" "query-terms:queryTerms" "probe:probe")
# ---- END CONFIG ---------------------------------------------------------------------------------

SCRIPT_NAME="$(basename "$0")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PARAM_CLI=()
PARAM_JMH=()
PARAM_VAL=()
for spec in "${PARAM_SPECS[@]}"; do
    PARAM_CLI+=("${spec%%:*}")
    PARAM_JMH+=("${spec#*:}")
    PARAM_VAL+=("")
done

ROOT=""
METHOD=""
EXCLUDE=""
FORKS=""
WARMUP_ITERATIONS=""
WARMUP_TIME=""
ITERATIONS=""
MEASUREMENT_TIME=""
THREADS=""
MODE=""
TIME_UNIT=""
JVM_ARGS=""
JVM_ARGS_APPEND=""
PROFILERS=()
EXTRA_ARGS=()
OUT_DIR="${DEFAULT_OUT_DIR}"
NAME=""
FORCE=false
CAFFEINATE=true
DRY_RUN=false
LIST_ONLY=false

usage() {
    local i=0
    cat <<EOF
Usage: ${SCRIPT_NAME} [options] [-- extra JMH arguments]

Runs ${BENCHMARK} through the ${RUNNER} launcher and writes <name>.txt, <name>.json and <name>.meta
to the output directory. Existing files are never overwritten unless --force is given.
Unset options keep the defaults declared on the benchmark class.

Benchmark parameters (comma separated lists run every combination):
EOF
    while [[ ${i} -lt ${#PARAM_CLI[@]} ]]; do
        printf '  --%-26s @Param %s\n' "${PARAM_CLI[$i]} LIST" "${PARAM_JMH[$i]}"
        i=$((i + 1))
    done
    cat <<EOF

Selection:
  --method NAME               run only the benchmark method NAME of the ${BENCHMARK} classes
  --exclude REGEX             JMH -e, skip matching benchmarks

JMH options:
  --forks N                   -f
  --warmup-iterations N       -wi
  --warmup-time TIME          -w   (for example 5s)
  --iterations N              -i
  --measurement-time TIME     -r   (for example 10s)
  --threads N                 -t
  --mode MODE                 -bm  (avgt, thrpt, sample, ss, all)
  --time-unit UNIT            -tu  (ns, us, ms, s, m, h)
  --jvm-args ARGS             -jvmArgs, replaces the forked JVM arguments
  --jvm-args-append ARGS      -jvmArgsAppend, added to the forked JVM arguments
  --profiler SPEC             -prof, repeatable (for example gc)
  -- ARGS...                  everything after -- goes to JMH unchanged

Output:
  --output-dir DIR            result directory (default: ${DEFAULT_OUT_DIR})
  --name NAME                 base file name (default: <benchmark>-<short sha>-<timestamp>)
  --force                     overwrite existing result files

Other:
  --root DIR                  repository root (default: git root of this script, then of the current directory)
  --list                      only list the benchmark and its parameters (JMH -lp), no measurement, no files
  --dry-run                   print the command and exit, no files
  --no-caffeinate             do not wrap the run in caffeinate (macOS)
  -h, --help                  show this help

Examples:
  ${SCRIPT_NAME} --forks 2 --iterations 10
  ${SCRIPT_NAME} --${PARAM_CLI[0]} A,B --profiler gc --name sweep-${PARAM_CLI[0]}
  ${SCRIPT_NAME} --dry-run -- -gc true
EOF
}

die() {
    echo "error: $*" >&2
    exit 2
}

require_value() {
    if [[ $# -lt 2 || -z "$2" ]]; then
        die "$1 requires a value"
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --root) require_value "$@"; ROOT="$2"; shift 2 ;;
        --method) require_value "$@"; METHOD="$2"; shift 2 ;;
        --exclude) require_value "$@"; EXCLUDE="$2"; shift 2 ;;
        --forks) require_value "$@"; FORKS="$2"; shift 2 ;;
        --warmup-iterations) require_value "$@"; WARMUP_ITERATIONS="$2"; shift 2 ;;
        --warmup-time) require_value "$@"; WARMUP_TIME="$2"; shift 2 ;;
        --iterations) require_value "$@"; ITERATIONS="$2"; shift 2 ;;
        --measurement-time) require_value "$@"; MEASUREMENT_TIME="$2"; shift 2 ;;
        --threads) require_value "$@"; THREADS="$2"; shift 2 ;;
        --mode) require_value "$@"; MODE="$2"; shift 2 ;;
        --time-unit) require_value "$@"; TIME_UNIT="$2"; shift 2 ;;
        --jvm-args) require_value "$@"; JVM_ARGS="$2"; shift 2 ;;
        --jvm-args-append) require_value "$@"; JVM_ARGS_APPEND="$2"; shift 2 ;;
        --profiler) require_value "$@"; PROFILERS+=("$2"); shift 2 ;;
        --output-dir) require_value "$@"; OUT_DIR="$2"; shift 2 ;;
        --name) require_value "$@"; NAME="$2"; shift 2 ;;
        --force) FORCE=true; shift ;;
        --list) LIST_ONLY=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --no-caffeinate) CAFFEINATE=false; shift ;;
        -h|--help) usage; exit 0 ;;
        --) shift; EXTRA_ARGS=("$@"); break ;;
        --*)
            matched=false
            i=0
            while [[ ${i} -lt ${#PARAM_CLI[@]} ]]; do
                if [[ "$1" == "--${PARAM_CLI[$i]}" ]]; then
                    require_value "$@"
                    PARAM_VAL[$i]="$2"
                    matched=true
                    shift 2
                    break
                fi
                i=$((i + 1))
            done
            [[ "${matched}" == true ]] || die "unknown option $1 (see --help)"
            ;;
        *) die "unexpected argument $1 (see --help)" ;;
    esac
done

[[ "${BENCHMARK}" != "CHANGE_ME" ]] || die "edit the CONFIG block: BENCHMARK is not set"
[[ "${RUNNER}" == "es" || "${RUNNER}" == "lucene" ]] || die "RUNNER must be es or lucene"

check_matches() {
    local option="$1" value="$2" pattern="$3" hint="$4"
    [[ -z "${value}" || "${value}" =~ ${pattern} ]] || die "${option} must be ${hint}, got '${value}'"
}
check_matches --forks "${FORKS}" '^[0-9]+$' "a non-negative integer"
check_matches --warmup-iterations "${WARMUP_ITERATIONS}" '^[0-9]+$' "a non-negative integer"
check_matches --iterations "${ITERATIONS}" '^[1-9][0-9]*$' "a positive integer"
check_matches --threads "${THREADS}" '^[1-9][0-9]*$|^max$' "a positive integer or max"
check_matches --warmup-time "${WARMUP_TIME}" '^[1-9][0-9]*(ns|us|ms|s|m|h)?$' "a duration such as 5s"
check_matches --measurement-time "${MEASUREMENT_TIME}" '^[1-9][0-9]*(ns|us|ms|s|m|h)?$' "a duration such as 10s"
check_matches --mode "${MODE}" '^(avgt|thrpt|sample|ss|all)$' "one of avgt, thrpt, sample, ss, all"
check_matches --time-unit "${TIME_UNIT}" '^(ns|us|ms|s|m|h)$' "one of ns, us, ms, s, m, h"
check_matches --name "${NAME}" '^[A-Za-z0-9._-]+$' "letters, digits, dot, underscore or hyphen"

if [[ -z "${ROOT}" ]]; then
    ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel 2>/dev/null || git rev-parse --show-toplevel 2>/dev/null || true)"
fi
[[ -n "${ROOT}" && -d "${ROOT}" ]] || die "cannot find the repository root, pass --root"
ROOT="$(cd "${ROOT}" && pwd)"
if [[ "${RUNNER}" == "es" ]]; then
    [[ -x "${ROOT}/gradlew" ]] || die "no executable gradlew in ${ROOT}"
elif [[ "${DRY_RUN}" == false && ! -d "${ROOT}/${LUCENE_MODULE_PATH}" ]]; then
    die "${ROOT}/${LUCENE_MODULE_PATH} not found, run ./gradlew :lucene:benchmark-jmh:assemble first"
fi

SELECTOR="${BENCHMARK}"
[[ -n "${METHOD}" ]] && SELECTOR="${BENCHMARK}.*[.]${METHOD}"

ARGS=("${SELECTOR}")
[[ -n "${EXCLUDE}" ]] && ARGS+=("-e" "${EXCLUDE}")
i=0
while [[ ${i} -lt ${#PARAM_CLI[@]} ]]; do
    [[ -n "${PARAM_VAL[$i]}" ]] && ARGS+=("-p" "${PARAM_JMH[$i]}=${PARAM_VAL[$i]}")
    i=$((i + 1))
done
[[ -n "${FORKS}" ]] && ARGS+=("-f" "${FORKS}")
[[ -n "${WARMUP_ITERATIONS}" ]] && ARGS+=("-wi" "${WARMUP_ITERATIONS}")
[[ -n "${WARMUP_TIME}" ]] && ARGS+=("-w" "${WARMUP_TIME}")
[[ -n "${ITERATIONS}" ]] && ARGS+=("-i" "${ITERATIONS}")
[[ -n "${MEASUREMENT_TIME}" ]] && ARGS+=("-r" "${MEASUREMENT_TIME}")
[[ -n "${THREADS}" ]] && ARGS+=("-t" "${THREADS}")
[[ -n "${MODE}" ]] && ARGS+=("-bm" "${MODE}")
[[ -n "${TIME_UNIT}" ]] && ARGS+=("-tu" "${TIME_UNIT}")
[[ -n "${JVM_ARGS}" ]] && ARGS+=("-jvmArgs" "${JVM_ARGS}")
[[ -n "${JVM_ARGS_APPEND}" ]] && ARGS+=("-jvmArgsAppend" "${JVM_ARGS_APPEND}")
for profiler in ${PROFILERS[@]+"${PROFILERS[@]}"}; do
    ARGS+=("-prof" "${profiler}")
done

SHA_SHORT="$(git -C "${ROOT}" rev-parse --short HEAD 2>/dev/null || echo nogit)"
STAMP="$(date +%Y%m%d-%H%M%S)"
SIMPLE_NAME="${BENCHMARK##*.}"
NAME="${NAME:-${SIMPLE_NAME}-${SHA_SHORT}-${STAMP}}"
case "${OUT_DIR}" in
    /*) ;;
    *) OUT_DIR="$(pwd)/${OUT_DIR}" ;;
esac
TXT="${OUT_DIR}/${NAME}.txt"
JSON="${OUT_DIR}/${NAME}.json"
META="${OUT_DIR}/${NAME}.meta"

if [[ "${LIST_ONLY}" == true ]]; then
    ARGS=("${SELECTOR}")
    [[ -n "${EXCLUDE}" ]] && ARGS+=("-e" "${EXCLUDE}")
    ARGS+=("-lp")
else
    ARGS+=("-rf" "json" "-rff" "${JSON}")
    ARGS+=(${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"})
fi

if [[ "${RUNNER}" == "es" ]]; then
    ARGS_STRING=""
    for arg in "${ARGS[@]}"; do
        [[ "${arg}" != *\"* ]] || die "JMH arguments must not contain a double quote: ${arg}"
        if [[ "${arg}" =~ [[:space:]] ]]; then
            arg="\"${arg}\""
        fi
        ARGS_STRING="${ARGS_STRING:+${ARGS_STRING} }${arg}"
    done
    CMD=("${ROOT}/gradlew" -p "${GRADLE_PROJECT_DIR}" run --args "${ARGS_STRING}")
    JMH_ARGS_SHOWN="${ARGS_STRING}"
else
    JAVA_BIN="${JAVA_HOME:+${JAVA_HOME}/bin/}java"
    CMD=("${JAVA_BIN}" --module-path "${ROOT}/${LUCENE_MODULE_PATH}" --module org.apache.lucene.benchmark.jmh "${ARGS[@]}")
    JMH_ARGS_SHOWN="${ARGS[*]}"
fi

if [[ "${LIST_ONLY}" == false && "${CAFFEINATE}" == true && "$(uname -s)" == "Darwin" ]]; then
    command -v caffeinate >/dev/null 2>&1 || die "caffeinate not found, install it or pass --no-caffeinate"
    CMD=(caffeinate -ims "${CMD[@]}")
fi

if [[ "${LIST_ONLY}" == false && "${DRY_RUN}" == false && "${FORCE}" == false ]]; then
    for existing in "${TXT}" "${JSON}" "${META}"; do
        [[ ! -e "${existing}" ]] || die "${existing} exists, use --name, --output-dir or --force"
    done
fi

if [[ "${DRY_RUN}" == true ]]; then
    echo "root:     ${ROOT}"
    [[ "${LIST_ONLY}" == true ]] || echo "txt:      ${TXT}"
    [[ "${LIST_ONLY}" == true ]] || echo "json:     ${JSON}"
    echo "jmh args: ${JMH_ARGS_SHOWN}"
    printf 'command:  '
    printf '%q ' "${CMD[@]}"
    echo
    exit 0
fi

cpu_model() {
    sysctl -n machdep.cpu.brand_string 2>/dev/null || sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo 2>/dev/null | head -1 || true
}

cpu_cores() {
    sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || true
}

memory_bytes() {
    sysctl -n hw.memsize 2>/dev/null || awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo 2>/dev/null || true
}

diff_digest() {
    git -C "${ROOT}" diff HEAD 2>/dev/null | { shasum 2>/dev/null || sha1sum 2>/dev/null || cksum; } | cut -d' ' -f1
}

write_meta() {
    {
        echo "timestamp_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "benchmark: ${SELECTOR}"
        echo "runner: ${RUNNER}"
        echo "commit: $(git -C "${ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
        echo "branch: $(git -C "${ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
        echo "diff_head_digest: $(diff_digest)"
        echo "changed_or_untracked_files:"
        git -C "${ROOT}" status --porcelain 2>/dev/null | sed -n '1,20p' | sed 's/^/  /'
        echo "os: $(uname -sr) $(uname -m)"
        echo "cpu: $(cpu_model)"
        echo "cores: $(cpu_cores)"
        echo "memory_bytes: $(memory_bytes)"
        if [[ "${RUNNER}" == "lucene" ]]; then
            echo "java: $("${JAVA_BIN}" -version 2>&1 | head -1)"
        else
            echo "java: recorded by JMH (jdkVersion in the JSON, '# VM version' in the txt)"
        fi
        echo "jmh_args: ${JMH_ARGS_SHOWN}"
        echo "script: ${SCRIPT_NAME} $*"
    } > "${META}"
}

if [[ "${LIST_ONLY}" == false ]]; then
    mkdir -p "${OUT_DIR}"
    write_meta "$@"
    echo "txt:  ${TXT}"
    echo "json: ${JSON}"
    echo "meta: ${META}"
    : > "${TXT}"
fi

cd "${ROOT}"
set +e
if [[ "${LIST_ONLY}" == true ]]; then
    "${CMD[@]}"
    status=$?
else
    set -o pipefail
    "${CMD[@]}" 2>&1 | tee -a "${TXT}"
    status=${PIPESTATUS[0]}
fi
set -e

if [[ "${LIST_ONLY}" == false ]]; then
    {
        echo "finished_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "exit_code: ${status}"
    } >> "${META}"
    if [[ ${status} -eq 0 && ! -s "${JSON}" ]]; then
        echo "warning: the run succeeded but ${JSON} is missing or empty" >&2
    fi
fi
exit "${status}"
