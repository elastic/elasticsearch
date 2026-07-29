#!/usr/bin/env bash
# Configures the source data stream with derived metrics, and creates Kibana data views for the
# source stream and its derived metrics destination.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=config.env
source "$HERE/config.env"

ES="http://localhost:${ES_PORT}"
AUTH=(-u "${ES_USER}:${ES_PASSWORD}")
KB="http://localhost:${KIBANA_PORT}"

es() {
  local method=$1 path=$2
  shift 2
  curl -sS -X "$method" "${AUTH[@]}" -H 'Content-Type: application/json' "${ES}${path}" "$@"
}


# The derived metrics configuration, defined once and used twice: baked into the index template for
# new data streams, and applied directly to the data stream so re-running this script reconfigures a
# demo that is already running.
DERIVED_METRICS=$(cat <<'JSON'
{
  "enabled": true,
  "builtin": ["ingest.*"],
  "intervals": ["10s"],
  "dimensions": ["service.name", "cloud.region"],
  "metrics": [
    {
      "name": "http.requests",
      "type": "counter",
      "when": { "exists": { "field": "http.request.method" } },
      "value": 1,
      "dimensions": ["http.request.method"]
    },
    {
      "name": "http.errors",
      "type": "counter",
      "when": { "range": { "http.response.status_code": { "gte": 500 } } },
      "value": 1,
      "dimensions": ["http.response.status_code"]
    },
    {
      "name": "http.client.errors",
      "type": "counter",
      "when": { "range": { "http.response.status_code": { "gte": 400, "lt": 500 } } },
      "value": 1
    },
    {
      "name": "http.response.bytes",
      "type": "counter",
      "value": { "field": "http.response.body.bytes" }
    },
    {
      "name": "queue.depth.max",
      "type": "gauge",
      "value": { "field": "queue.depth" },
      "aggregation": "max"
    },
    {
      "name": "queue.depth.last",
      "type": "gauge",
      "value": { "field": "queue.depth" },
      "aggregation": "last_value"
    },
    {
      "name": "event.duration.avg",
      "type": "gauge",
      "value": { "field": "event.duration" },
      "aggregation": "avg"
    }
  ]
}
JSON
)

echo "==> Creating index template [${INDEX_TEMPLATE}] for [${DATA_STREAM}]"
# The derived metrics configuration lives in data_stream_options, so it is inherited by the data
# stream when it is created. Priority 200 beats the built-in logs-*-* template.
es PUT "/_index_template/${INDEX_TEMPLATE}" -d @- <<JSON | python3 -m json.tool
{
  "index_patterns": ["${DATA_STREAM}"],
  "priority": 200,
  "data_stream": {},
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    },
    "mappings": {
      "properties": {
        "@timestamp":                 { "type": "date" },
        "message":                    { "type": "text" },
        "service":   { "properties": { "name": { "type": "keyword" } } },
        "cloud":     { "properties": { "region": { "type": "keyword" } } },
        "host":      { "properties": { "name": { "type": "keyword" } } },
        "event":     { "properties": { "duration": { "type": "long" }, "outcome": { "type": "keyword" } } },
        "queue":     { "properties": { "depth": { "type": "long" } } },
        "http": {
          "properties": {
            "request":  { "properties": { "method": { "type": "keyword" } } },
            "response": { "properties": { "status_code": { "type": "short" }, "body": { "properties": { "bytes": { "type": "long" } } } } }
          }
        }
      }
    },
    "data_stream_options": {
      "derived_metrics": ${DERIVED_METRICS}
    }
  }
}
JSON

echo "==> Creating data stream [${DATA_STREAM}]"
es PUT "/_data_stream/${DATA_STREAM}" | python3 -m json.tool || true

# Options inherited from a template only apply when the data stream is created, so an existing
# stream has to be reconfigured explicitly. The runtime picks this up on the next cluster state
# update, with no restart.
echo "==> Applying derived metrics to the live data stream"
es PUT "/_data_stream/${DATA_STREAM}/_options" -d "{\"derived_metrics\": ${DERIVED_METRICS}}" | python3 -m json.tool

echo "==> Effective derived metrics configuration"
es GET "/_data_stream/${DATA_STREAM}/_options" | python3 -m json.tool

# Kibana data views are optional: the demo is still usable through the ES API without them.
if curl -sS -m 5 -o /dev/null "${KB}/api/status" 2>/dev/null; then
  create_data_view() {
    local title=$1 name=$2
    echo "==> Creating Kibana data view [${name}] for [${title}]"
    # allowHidden matters for the destination, which is a hidden data stream.
    curl -sS -X POST "${KB}/api/data_views/data_view" \
      "${AUTH[@]}" \
      -H 'Content-Type: application/json' \
      -H 'kbn-xsrf: true' \
      -d "{\"data_view\":{\"title\":\"${title}\",\"name\":\"${name}\",\"timeFieldName\":\"@timestamp\",\"allowHidden\":true}}" \
      | python3 -c 'import sys,json
try:
    r = json.load(sys.stdin)
except Exception:
    sys.exit(0)
if "data_view" in r:
    print("    created:", r["data_view"]["id"])
else:
    print("    skipped:", r.get("message", r))'
  }
  create_data_view "${DATA_STREAM}" "demo source stream"
  create_data_view "derived-metrics-${DATA_STREAM}" "demo derived metrics"

  echo "==> Creating the comparison dashboard"
  python3 "$HERE/dashboard.py" \
    --kibana "${KB}" --user "${ES_USER}" --password "${ES_PASSWORD}" \
    --data-stream "${DATA_STREAM}"
else
  echo "==> Kibana is not reachable at ${KB}, skipping data view creation"
fi

echo
echo "Setup complete."
