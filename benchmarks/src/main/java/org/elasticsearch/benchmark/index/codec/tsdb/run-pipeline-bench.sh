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
#   Tier 3: [PIPELINE] PipelineSelector decision-tree paths
#           The 13 pipelines that PipelineSelector actually selects for doubles.
#           Compare with Tier 2 to measure algorithm complexity differences.
#
# Analysis enabled per file:
#   Tier 1 vs Tier 2 → pipeline architecture throughput overhead (same algorithm)
#   Tier 2 vs Tier 3 → algorithm complexity delta (same architecture)
#
# Pipeline variants tested (13 from PipelineSelector + 1 ES819-pipeline-reference):
#   delta-offset-gcd-bitpack (ES819-equivalent, Tier 2),
#   integer-pipeline,
#   alp-double-lossless, alp-double-1e4, alp-double-1e2,
#   fpc-lossless, fpc-1e4, fpc-1e2,
#   gorilla-lossless, gorilla-1e4, gorilla-1e2,
#   chimp-lossless, chimp-1e4, chimp128-1e2
#
# Total commands: 450
#   = (1 ES819-baseline + 14 pipeline variants) x 30 double datasets
#
# Output filtering:
#   - Gradle runs with --quiet to suppress build output.
#   - JMH output is filtered to keep only the summary table (header + result rows).
#   - Per-iteration warmup/measurement lines are excluded.
#
# Progress:
#   After each command completes, a status line "completed X/450 (Xs)" is printed
#   showing elapsed seconds for that command.
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
                Other values match the JMH pipeline param directly (e.g. alp-double-lossless).
                Examples:
                  $0 -o results -p ES819-baseline,alp-double-lossless
                  $0 -o results -d sensor-2dp-double -p ES819-baseline,ES819-pipeline-reference,fpc-lossless
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

# Count pipelines per dataset: from -p list or all (1 baseline + 14 pipeline variants)
if [[ -n "${PIPELINE_LIST}" ]]; then
  IFS=',' read -ra _pl <<< "${PIPELINE_LIST}"
  NUM_PIPELINES=${#_pl[@]}
else
  NUM_PIPELINES=15
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

DATASETS=(
  constant-double
  percentage-double
  monotonic-double
  gauge-double
  realistic-gauge-double
  sparse-gauge-double
  random-double
  stable-sensor-double
  tiny-increment-double
  steady-counter-double
  burst-spike-double
  zero-crossing-oscillation-double
  step-with-spikes-double
  counter-with-resets-double
  quantized-double
  sensor-2dp-double
  temperature-1dp-double
  financial-2dp-double
  percentage-rounded-1dp-double
  mixed-sign-double
  step-hold-double
  timestamp-as-double
  counter-as-double
  gauge-as-double
  gcd-as-double
  constant-as-double
  random-as-double
  decreasing-timestamp-as-double
  small-as-double
  timestamp-with-jitter-as-double
)

# The 13 pipelines from PipelineSelector decision tree
SELECTOR_PIPELINES=(
  integer-pipeline
  alp-double-lossless
  alp-double-1e4
  alp-double-1e2
  fpc-lossless
  fpc-1e4
  fpc-1e2
  gorilla-lossless
  gorilla-1e4
  gorilla-1e2
  chimp-lossless
  chimp-1e4
  chimp128-1e2
)

for dataset in "${DATASETS[@]}"; do
  skip_dataset "${dataset}" && continue

  # [ES819-baseline] ES819 production codec (TSDBDocValuesEncoder)
  run "(Encode|Decode)DoubleGeneratorsBenchmark -p datasetName=${dataset} -wi ${WI} -i ${I} -f ${F}" "${dataset}.txt"

  # [ES819-pipeline-reference] delta-offset-gcd-bitpack (pipeline architecture overhead)
  run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=${dataset} -p pipeline=delta-offset-gcd-bitpack -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "${dataset}.txt"

  # [PIPELINE] PipelineSelector decision-tree paths
  for pipeline in "${SELECTOR_PIPELINES[@]}"; do
    run "(Encode|Decode)DoubleGeneratorsPipelineBenchmark -p datasetName=${dataset} -p pipeline=${pipeline} -p blockSize=${BS} -wi ${WI} -i ${I} -f ${F}" "${dataset}.txt"
  done

  merge_dataset_json "${dataset}"
done

# Clean up temp directory
rmdir "${OUTDIR}/.tmp" 2>/dev/null || true

echo "Done in $((SECONDS - SCRIPT_START))s. Results in: ${OUTDIR}"
