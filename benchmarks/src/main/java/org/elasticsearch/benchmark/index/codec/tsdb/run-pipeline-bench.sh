#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# ES819 vs Pipeline Throughput Comparison (double datasets only)
# ──────────────────────────────────────────────────────────────────────────────
#
# Purpose:
#   Compare the ES819 codec (TSDBDocValuesEncoder) against the new pipeline codec
#   across all 30 double synthetic datasets from NumericDataGenerators.
#   Each dataset is benchmarked for both encode and decode throughput in a single
#   JMH invocation using the (Encode|Decode) regex pattern.
#
# Output structure:
#   One file per dataset (e.g. constant-double.txt) in the OUTDIR directory.
#   Each file contains three tiers of results:
#
#   Tier 1: [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
#           The current production encode/decode implementation.
#
#   Tier 2: [ES819-pipeline-reference] delta-offset-gcd-bitpack pipeline
#           The ES819-equivalent algorithm running through the pipeline architecture.
#           Compare with Tier 1 to measure pipeline architecture overhead.
#
#   Tier 3: [PIPELINE] Type-specific pipeline variants
#           Different algorithms running through the same pipeline architecture.
#           Compare with Tier 2 to measure algorithm complexity differences.
#
# Analysis enabled per file:
#   Tier 1 vs Tier 2 → pipeline architecture throughput overhead (same algorithm)
#   Tier 2 vs Tier 3 → algorithm complexity delta (same architecture)
#
# Pipeline variants tested (double, 19 from DOUBLE_SHORTLIST):
#   alp_double, alp_rd_double,
#   alp_double_stage-offset-gcd-bitpack, alp_rd_double_stage-offset-gcd-bitpack,
#   quantize-1e6-delta-offset-gcd-bitpack, alp_double_stage-1e6-offset-gcd-bitpack,
#   gorilla, xor-bitpack, offset-gcd-bitpack, zstd,
#   delta-offset-gcd-bitpack (ES819-equivalent, Tier 2), delta-offset-gcd-zstd,
#   alp_double_stage-offset-gcd-zstd, alp_double_stage-1e6-zstd,
#   fpc-offset-gcd-bitpack, fpc-bitpack, fpc-zstd, fpc-offset-gcd-zstd,
#   quantize-1e6-fpc-offset-gcd-bitpack
#
# Total commands: 600
#   = (1 ES819-baseline + 19 pipeline variants) x 30 double datasets
#
# Output filtering:
#   - Gradle runs with --quiet to suppress build output.
#   - JMH output is filtered to keep only the summary table (header + result rows).
#   - Per-iteration warmup/measurement lines are excluded.
#
# Progress:
#   After each command completes, a status line "completed X/480 (Xs)" is printed
#   showing elapsed seconds for that command.
#
# Copy-paste:
#   Every `run` line is preceded by a comment with the fully expanded ./gradlew command.
#   You can copy any single comment line and run it standalone in a terminal.
#
# JMH defaults: warmup=5, measurement=5, forks=1, blockSize=128 (override with -w, -i, -f, -b)
# ──────────────────────────────────────────────────────────────────────────────
#set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 -o <dir> [-d <filter>] [-p <filter>] [-b <n>] [-w <n>] [-i <n>] [-f <n>] [-n] [-h]

Options:
  -o <dir>      Output directory name (required). Created under the benchmark results path.
                Example: $0 -o results-20260202
  -d <list>     Comma-separated list of dataset names to run (exact match).
                Examples:
                  $0 -o results -d sensor-2dp-double
                  $0 -o results -d gauge-double,constant-double
                Without -d, all 30 datasets are run.
  -p <list>     Comma-separated list of pipeline names to run (exact match).
                Special names:
                  ES819-baseline             TSDBDocValuesEncoder (non-pipeline)
                  ES819-pipeline-reference   delta-offset-gcd-bitpack via pipeline
                Other values match the JMH pipeline param directly (e.g. alp_double).
                Examples:
                  $0 -o results -p ES819-baseline,alp_double
                  $0 -o results -d sensor-2dp-double -p ES819-baseline,ES819-pipeline-reference,alp_double
                Without -p, all pipelines are run.
  -b <n>        Block size for pipeline codecs (default: 128). ES819-baseline always uses 128.
  -w <n>        JMH warmup iterations (default: 5).
  -i <n>        JMH measurement iterations (default: 5).
  -f <n>        JMH forks (default: 1).
  -n            Dry run. Print the commands that would be executed without running them.
  -h            Show this help and exit.
EOF
}

DATASET_LIST=""
PIPELINE_LIST=""
DRY_RUN=false
OUT_NAME=""
OPT_BS=""
OPT_WI=""
OPT_I=""
OPT_F=""
while getopts "d:p:o:b:w:i:f:nh" opt; do
  case "${opt}" in
    d) DATASET_LIST="${OPTARG}" ;;
    p) PIPELINE_LIST="${OPTARG}" ;;
    o) OUT_NAME="${OPTARG}" ;;
    b) OPT_BS="${OPTARG}" ;;
    w) OPT_WI="${OPTARG}" ;;
    i) OPT_I="${OPTARG}" ;;
    f) OPT_F="${OPTARG}" ;;
    n) DRY_RUN=true ;;
    h) usage; exit 0 ;;
    *) usage; exit 1 ;;
  esac
done

if [[ -z "${OUT_NAME}" ]]; then
  echo "Error: -o <dir> is required." >&2
  usage
  exit 1
fi

cd "$(git rev-parse --show-toplevel)"
OUTDIR="$(pwd)/benchmarks/src/main/java/org/elasticsearch/benchmark/index/codec/tsdb/${OUT_NAME}"
mkdir -p "${OUTDIR}"
if [[ -z "${DATASET_LIST}" ]]; then
  rm -f "${OUTDIR}"/*.txt
fi
echo "Output directory: ${OUTDIR}"
if [[ -n "${DATASET_LIST}" ]]; then
  echo "Datasets: ${DATASET_LIST}"
fi
if [[ -n "${PIPELINE_LIST}" ]]; then
  echo "Pipelines: ${PIPELINE_LIST}"
fi

SCRIPT_START=$SECONDS
COMPLETED=0

# Count datasets: from -d list or all 30
if [[ -n "${DATASET_LIST}" ]]; then
  IFS=',' read -ra _ds <<< "${DATASET_LIST}"
  NUM_DATASETS=${#_ds[@]}
else
  NUM_DATASETS=30
fi

# Count pipelines per dataset: from -p list or all (1 baseline + 19 DOUBLE_SHORTLIST)
if [[ -n "${PIPELINE_LIST}" ]]; then
  IFS=',' read -ra _pl <<< "${PIPELINE_LIST}"
  NUM_PIPELINES=${#_pl[@]}
else
  NUM_PIPELINES=20
fi

TOTAL=$(( NUM_DATASETS * NUM_PIPELINES ))

# Skip datasets not in the -d list (exact match, comma-separated).
skip_dataset() {
  local dataset="$1"
  [[ -z "${DATASET_LIST}" ]] && return 1
  IFS=',' read -ra _ds <<< "${DATASET_LIST}"
  for _d in "${_ds[@]}"; do
    [[ "${_d}" == "${dataset}" ]] && return 1
  done
  return 0
}

# Track run index within current dataset (reset after each merge)
RUN_INDEX=0

# Merge all temp JSON files for a dataset into one valid JSON array
merge_dataset_json() {
  local dataset="$1"
  local jsonfile="${OUTDIR}/${dataset}.json"
  local tmpdir="${OUTDIR}/.tmp"
  local tmpfiles=("${tmpdir}/${dataset}"-*.json)

  # Reset run index for next dataset (always, even in dry-run)
  RUN_INDEX=0

  if [[ ! -e "${tmpfiles[0]}" ]]; then
    return
  fi

  # Merge new temp files with existing JSON (if any) to support incremental runs
  if [[ -f "${jsonfile}" ]]; then
    jq -s 'add' "${jsonfile}" "${tmpfiles[@]}" > "${jsonfile}.new" && mv "${jsonfile}.new" "${jsonfile}"
  else
    jq -s 'add' "${tmpfiles[@]}" > "${jsonfile}"
  fi
  rm -f "${tmpfiles[@]}"
  echo "  merged ${#tmpfiles[@]} results into ${dataset}.json"
}

run() {
  local args="$1"
  local outfile="${OUTDIR}/$2"
  local dataset="${2%.txt}"

  # Pipeline filtering: extract logical pipeline name from JMH args.
  if [[ -n "${PIPELINE_LIST}" ]]; then
    local pipeline_name
    if [[ "${args}" =~ pipeline=([^ ]+) ]]; then
      pipeline_name="${BASH_REMATCH[1]}"
      if [[ "${pipeline_name}" == "delta-offset-gcd-bitpack" ]]; then
        pipeline_name="ES819-pipeline-reference"
      fi
    else
      pipeline_name="ES819-baseline"
    fi
    local _matched=false
    IFS=',' read -ra _pl <<< "${PIPELINE_LIST}"
    for _p in "${_pl[@]}"; do
      [[ "${_p}" == "${pipeline_name}" ]] && _matched=true
    done
    if ! "${_matched}"; then
      return
    fi
  fi

  local tmpdir="${OUTDIR}/.tmp"
  mkdir -p "${tmpdir}"
  local tmpjson="${tmpdir}/${dataset}-$(printf '%03d' ${RUN_INDEX}).json"
  RUN_INDEX=$((RUN_INDEX + 1))

  COMPLETED=$((COMPLETED + 1))
  if "${DRY_RUN}"; then
    echo "[dry-run ${COMPLETED}/${TOTAL}] ./gradlew --quiet :benchmarks:run --args='${args} -rf json -rff ${tmpjson}' >> ${outfile}"
    return
  fi
  local start_ts=$SECONDS
  echo ">>> ${args}" | tee -a "${outfile}"
  ./gradlew --quiet :benchmarks:run --args="${args} -rf json -rff ${tmpjson}" 2>&1 \
    | { grep -E '^(Benchmark |.*\b(thrpt|avgt|sample|ss|all)\b.*[0-9])' || true; } \
    | tee -a "${outfile}"
  echo "" | tee -a "${outfile}"
  local duration=$(( SECONDS - start_ts ))
  echo "completed ${COMPLETED}/${TOTAL} (${duration}s)"
}

BS=${OPT_BS:-128}; WI=${OPT_WI:-5}; I=${OPT_I:-5}; F=${OPT_F:-1}
echo "JMH settings: warmup=${WI}, measurement=${I}, forks=${F}, blockSize=${BS}"
# ── DOUBLE ───────────────────────────────────────────────────────────────────

# DATASET: constant-double
skip_dataset "constant-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=constant-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=constant-double -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-double.txt"

}
merge_dataset_json "constant-double"

# DATASET: percentage-double
skip_dataset "percentage-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=percentage-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=percentage-double -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-double.txt"

}
merge_dataset_json "percentage-double"

# DATASET: monotonic-double
skip_dataset "monotonic-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=monotonic-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=monotonic-double -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=monotonic-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "monotonic-double.txt"

}
merge_dataset_json "monotonic-double"

# DATASET: gauge-double
skip_dataset "gauge-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=gauge-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=gauge-double -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-double.txt"

}
merge_dataset_json "gauge-double"

# DATASET: realistic-gauge-double
skip_dataset "realistic-gauge-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=realistic-gauge-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=realistic-gauge-double -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=realistic-gauge-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "realistic-gauge-double.txt"

}
merge_dataset_json "realistic-gauge-double"

# DATASET: sparse-gauge-double
skip_dataset "sparse-gauge-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=sparse-gauge-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=sparse-gauge-double -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sparse-gauge-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sparse-gauge-double.txt"

}
merge_dataset_json "sparse-gauge-double"

# DATASET: random-double
skip_dataset "random-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=random-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=random-double -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-double.txt"

}
merge_dataset_json "random-double"

# DATASET: stable-sensor-double
skip_dataset "stable-sensor-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=stable-sensor-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=stable-sensor-double -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=stable-sensor-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "stable-sensor-double.txt"

}
merge_dataset_json "stable-sensor-double"

# DATASET: tiny-increment-double
skip_dataset "tiny-increment-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=tiny-increment-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=tiny-increment-double -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=tiny-increment-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "tiny-increment-double.txt"

}
merge_dataset_json "tiny-increment-double"

# DATASET: steady-counter-double
skip_dataset "steady-counter-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=steady-counter-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=steady-counter-double -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=steady-counter-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "steady-counter-double.txt"

}
merge_dataset_json "steady-counter-double"

# DATASET: burst-spike-double
skip_dataset "burst-spike-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=burst-spike-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=burst-spike-double -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=burst-spike-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "burst-spike-double.txt"

}
merge_dataset_json "burst-spike-double"

# DATASET: zero-crossing-oscillation-double
skip_dataset "zero-crossing-oscillation-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=zero-crossing-oscillation-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=zero-crossing-oscillation-double -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=zero-crossing-oscillation-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "zero-crossing-oscillation-double.txt"

}
merge_dataset_json "zero-crossing-oscillation-double"

# DATASET: step-with-spikes-double
skip_dataset "step-with-spikes-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=step-with-spikes-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=step-with-spikes-double -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-with-spikes-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-with-spikes-double.txt"

}
merge_dataset_json "step-with-spikes-double"

# DATASET: counter-with-resets-double
skip_dataset "counter-with-resets-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=counter-with-resets-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=counter-with-resets-double -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-with-resets-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-with-resets-double.txt"

}
merge_dataset_json "counter-with-resets-double"

# DATASET: quantized-double
skip_dataset "quantized-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=quantized-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=quantized-double -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=quantized-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "quantized-double.txt"

}
merge_dataset_json "quantized-double"

# DATASET: sensor-2dp-double
skip_dataset "sensor-2dp-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=sensor-2dp-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=sensor-2dp-double -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=sensor-2dp-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "sensor-2dp-double.txt"

}
merge_dataset_json "sensor-2dp-double"

# DATASET: temperature-1dp-double
skip_dataset "temperature-1dp-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=temperature-1dp-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=temperature-1dp-double -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=temperature-1dp-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "temperature-1dp-double.txt"

}
merge_dataset_json "temperature-1dp-double"

# DATASET: financial-2dp-double
skip_dataset "financial-2dp-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=financial-2dp-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=financial-2dp-double -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=financial-2dp-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "financial-2dp-double.txt"

}
merge_dataset_json "financial-2dp-double"

# DATASET: percentage-rounded-1dp-double
skip_dataset "percentage-rounded-1dp-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=percentage-rounded-1dp-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=percentage-rounded-1dp-double -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=percentage-rounded-1dp-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "percentage-rounded-1dp-double.txt"

}
merge_dataset_json "percentage-rounded-1dp-double"

# DATASET: mixed-sign-double
skip_dataset "mixed-sign-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=mixed-sign-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=mixed-sign-double -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=mixed-sign-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "mixed-sign-double.txt"

}
merge_dataset_json "mixed-sign-double"

# DATASET: step-hold-double
skip_dataset "step-hold-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=step-hold-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=step-hold-double -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=step-hold-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "step-hold-double.txt"

}
merge_dataset_json "step-hold-double"

# DATASET: timestamp-as-double
skip_dataset "timestamp-as-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=timestamp-as-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=timestamp-as-double -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-as-double.txt"

}
merge_dataset_json "timestamp-as-double"

# DATASET: counter-as-double
skip_dataset "counter-as-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=counter-as-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=counter-as-double -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=counter-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "counter-as-double.txt"

}
merge_dataset_json "counter-as-double"

# DATASET: gauge-as-double
skip_dataset "gauge-as-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=gauge-as-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=gauge-as-double -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gauge-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gauge-as-double.txt"

}
merge_dataset_json "gauge-as-double"

# DATASET: gcd-as-double
skip_dataset "gcd-as-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=gcd-as-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=gcd-as-double -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=gcd-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "gcd-as-double.txt"

}
merge_dataset_json "gcd-as-double"

# DATASET: constant-as-double
skip_dataset "constant-as-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=constant-as-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=constant-as-double -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=constant-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "constant-as-double.txt"

}
merge_dataset_json "constant-as-double"

# DATASET: random-as-double
skip_dataset "random-as-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=random-as-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=random-as-double -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=random-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "random-as-double.txt"

}
merge_dataset_json "random-as-double"

# DATASET: decreasing-timestamp-as-double
skip_dataset "decreasing-timestamp-as-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=decreasing-timestamp-as-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=decreasing-timestamp-as-double -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=decreasing-timestamp-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "decreasing-timestamp-as-double.txt"

}
merge_dataset_json "decreasing-timestamp-as-double"

# DATASET: small-as-double
skip_dataset "small-as-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=small-as-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=small-as-double -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=small-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "small-as-double.txt"

}
merge_dataset_json "small-as-double"

# DATASET: timestamp-with-jitter-as-double
skip_dataset "timestamp-with-jitter-as-double" || {
# [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=timestamp-with-jitter-as-double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=timestamp-with-jitter-as-double -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] alp_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] alp_rd_double
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_rd_double -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_rd_double -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] alp_rd_double_stage-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_rd_double_stage-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] quantize-1e6-delta-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=quantize-1e6-delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_double_stage-1e6-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] gorilla
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=gorilla -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=gorilla -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] xor-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=xor-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=xor-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] delta-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=delta-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=delta-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] alp_double_stage-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_double_stage-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] alp_double_stage-1e6-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_double_stage-1e6-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=alp_double_stage-1e6-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] fpc-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=fpc-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=fpc-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] fpc-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=fpc-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=fpc-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] fpc-offset-gcd-zstd
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=fpc-offset-gcd-zstd -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=fpc-offset-gcd-zstd -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

# [PIPELINE] quantize-1e6-fpc-offset-gcd-bitpack
# ./gradlew :benchmarks:run --args='(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -wi ${WI} -i ${I} -f ${F}'
run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=timestamp-with-jitter-as-double -p pipeline=quantize-1e6-fpc-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "timestamp-with-jitter-as-double.txt"

}
merge_dataset_json "timestamp-with-jitter-as-double"

# Clean up temp directory
rmdir "${OUTDIR}/.tmp" 2>/dev/null || true

echo "Done in $((SECONDS - SCRIPT_START))s. Results in: ${OUTDIR}"
