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
  "enabled": __ENABLED__,
  "builtin": ["ingest.*"],
  "default_interval": "__INTERVAL__",
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
      "name": "event.duration.avg",
      "type": "gauge",
      "value": { "field": "event.duration" },
      "aggregation": "avg"
    },
    {
      "name": "event.duration.distribution",
      "type": "histogram",
      "value": { "field": "event.duration" }
    }
  ]
}
JSON
)
DERIVED_METRICS=${DERIVED_METRICS/__INTERVAL__/${DEFAULT_INTERVAL}}
DERIVED_METRICS=${DERIVED_METRICS/__ENABLED__/${DERIVED_METRICS_ENABLED}}

if [[ "${DERIVED_METRICS_ENABLED}" != "true" ]]; then
  echo "==> Derived metrics are DISABLED for this run (control)"
fi

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


# The lean stream receives exactly the same documents but asks for far less: one built-in counter,
# two user metrics, and a single dimension. Its derived volume is a fraction of the rich stream's,
# which is the point of the comparison, while still being broken down per service.
LEAN_DERIVED_METRICS=$(cat <<'JSON'
{
  "enabled": __ENABLED__,
  "builtin": ["ingest.docs.count"],
  "default_interval": "__INTERVAL__",
  "dimensions": ["service.name"],
  "metrics": [
    {
      "name": "http.errors",
      "type": "counter",
      "when": { "range": { "http.response.status_code": { "gte": 500 } } },
      "value": 1
    },
    {
      "name": "queue.depth.max",
      "type": "gauge",
      "value": { "field": "queue.depth" },
      "aggregation": "max"
    }
  ]
}
JSON
)
LEAN_DERIVED_METRICS=${LEAN_DERIVED_METRICS/__INTERVAL__/${DEFAULT_INTERVAL}}
LEAN_DERIVED_METRICS=${LEAN_DERIVED_METRICS/__ENABLED__/${DERIVED_METRICS_ENABLED}}

echo "==> Creating index template [${LEAN_INDEX_TEMPLATE}] for [${LEAN_DATA_STREAM}]"
es PUT "/_index_template/${LEAN_INDEX_TEMPLATE}" -d @- <<JSON | python3 -m json.tool
{
  "index_patterns": ["${LEAN_DATA_STREAM}"],
  "priority": 200,
  "data_stream": {},
  "template": {
    "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
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
    "data_stream_options": { "derived_metrics": ${LEAN_DERIVED_METRICS} }
  }
}
JSON

echo "==> Creating data stream [${LEAN_DATA_STREAM}]"
es PUT "/_data_stream/${LEAN_DATA_STREAM}" | python3 -m json.tool || true

echo "==> Applying derived metrics to the live lean data stream"
es PUT "/_data_stream/${LEAN_DATA_STREAM}/_options" -d "{\"derived_metrics\": ${LEAN_DERIVED_METRICS}}" | python3 -m json.tool

echo "==> Effective derived metrics configuration"
es GET "/_data_stream/${DATA_STREAM}/_options" | python3 -m json.tool

# Kibana is optional: the demo is usable through the ES API without it, and it may still be starting.
# Its bootstrap lives in its own script so it can be re-run on demand with ./demo.sh bootstrap-kibana.
if [[ "${KIBANA:-}" == "skip" ]]; then
  # Without this the bootstrap waits its full timeout for a Kibana that was never started.
  echo "==> KIBANA=skip, so no data views or dashboards"
elif ! bash "$HERE/bootstrap-kibana.sh"; then
  echo "==> Skipping Kibana for now; run ./demo.sh bootstrap-kibana once it is ready"
fi

echo
echo "Setup complete."
