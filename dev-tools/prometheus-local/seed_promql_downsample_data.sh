#!/usr/bin/env bash
set -euo pipefail

ES_URL="${ES_URL:-http://localhost:9200}"
ES_AUTH="${ES_AUTH:-elastic:password}"
RAW_INDEX="${RAW_INDEX:-promql-downsample-raw}"
START="${START:-2024-05-10T00:00:00Z}"
END="${END:-2024-05-10T18:00:00Z}"
QUERY_END="${QUERY_END:-2024-05-10T16:40:00Z}"
SAMPLE_COUNT="${SAMPLE_COUNT:-100}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-600}"
PREVIEW_QUERY="${PREVIEW_QUERY:-max_over_time(rx[1h])}"
PREVIEW_STEP="${PREVIEW_STEP:-1h}"
readonly DOWNSAMPLE_SPECS=(
  "30m:promql-downsample-30m"
  "1h:promql-downsample-1h"
  "2h:promql-downsample-2h"
)
TMP_BULK=""

cleanup() {
  if [ -n "${TMP_BULK}" ]; then
    rm -f "${TMP_BULK}"
  fi
}

trap cleanup EXIT

curl_es() {
  curl -fsS -u "${ES_AUTH}" -H "Content-Type: application/json" "$@"
}

index_names() {
  local indexes="${RAW_INDEX}"

  for spec in "${DOWNSAMPLE_SPECS[@]}"; do
    indexes="${indexes},${spec#*:}"
  done

  printf '%s\n' "${indexes}"
}

sample_epoch() {
  date -j -u -f "%Y-%m-%dT%H:%M:%SZ" "${START}" "+%s"
}

sample_timestamp() {
  local epoch="$1"
  date -j -u -r "${epoch}" "+%Y-%m-%dT%H:%M:%SZ"
}

sample_value() {
  local sample="$1"

  if [ "${sample}" -lt 35 ]; then
    printf '%s\n' $((18000 + sample * 1400))
  elif [ "${sample}" -lt 70 ]; then
    printf '%s\n' $((67000 + (sample - 35) * 3300))
  elif [ "${sample}" -lt 84 ]; then
    printf '%s\n' $((182500 + (sample - 70) * 7200))
  else
    printf '%s\n' $((283300 - (sample - 84) * 5200))
  fi
}

reset_indices() {
  local targets
  targets="$(index_names)"

  echo "Resetting [${targets}]"
  curl_es -X DELETE "${ES_URL}/${targets}?ignore_unavailable=true" >/dev/null
}

create_raw_index() {
  curl_es -X PUT "${ES_URL}/${RAW_INDEX}" -d "{
    \"settings\": {
      \"number_of_shards\": 1,
      \"index\": {
        \"mode\": \"time_series\",
        \"routing_path\": [\"cluster\", \"pod\"],
        \"time_series\": {
          \"start_time\": \"${START}\",
          \"end_time\": \"${END}\"
        }
      }
    },
    \"mappings\": {
      \"properties\": {
        \"@timestamp\": { \"type\": \"date\" },
        \"cluster\": { \"type\": \"keyword\", \"time_series_dimension\": true },
        \"pod\": { \"type\": \"keyword\", \"time_series_dimension\": true },
        \"rx\": { \"type\": \"double\", \"time_series_metric\": \"gauge\" }
      }
    }
  }" >/dev/null
}

seed_raw_samples() {
  local base_epoch
  TMP_BULK="$(mktemp)"
  base_epoch="$(sample_epoch)"

  for sample in $(seq 0 $((SAMPLE_COUNT - 1))); do
    local epoch
    local timestamp
    local value
    epoch=$((base_epoch + sample * SAMPLE_INTERVAL_SECONDS))
    timestamp="$(sample_timestamp "${epoch}")"
    value="$(sample_value "${sample}")"

    cat >> "${TMP_BULK}" <<EOF
{"index":{"_index":"${RAW_INDEX}"}}
{"@timestamp":"${timestamp}","cluster":"prod","pod":"one","rx":${value}}
EOF
  done

  curl_es -X POST "${ES_URL}/_bulk?refresh=true" --data-binary "@${TMP_BULK}" >/dev/null
}

create_downsample_targets() {
  curl_es -X PUT "${ES_URL}/${RAW_INDEX}/_settings" -d '{"index.blocks.write":true}' >/dev/null

  for spec in "${DOWNSAMPLE_SPECS[@]}"; do
    local interval="${spec%%:*}"
    local target="${spec#*:}"

    echo "Creating [${target}] with fixed_interval [${interval}]"
    curl_es -X POST "${ES_URL}/${RAW_INDEX}/_downsample/${target}" -d "{
      \"fixed_interval\": \"${interval}\",
      \"sampling_method\": \"aggregate\"
    }" >/dev/null
  done
}

preview_query() {
  local index="$1"
  local label="$2"

  echo
  echo "${label}:"
  curl_es --get "${ES_URL}/_prometheus/${index}/api/v1/query_range" \
    --data-urlencode "query=${PREVIEW_QUERY}" \
    --data-urlencode "start=${START}" \
    --data-urlencode "end=${QUERY_END}" \
    --data-urlencode "step=${PREVIEW_STEP}"
}

main() {
  reset_indices
  create_raw_index
  seed_raw_samples
  create_downsample_targets

  echo "Seeded PromQL downsample comparison data"
  preview_query "${RAW_INDEX}" "Raw [${RAW_INDEX}]"
  for spec in "${DOWNSAMPLE_SPECS[@]}"; do
    preview_query "${spec#*:}" "Downsampled [${spec#*:}]"
  done
  echo
}

main "$@"
