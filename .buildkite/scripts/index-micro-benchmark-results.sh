#!/bin/bash

METRICS_HOST=$(vault read -field=es_host /secret/performance/employees/cloud/esbench-metrics)
METRICS_INDEX_NAME="dummy-micro-benchmarks"
METRICS_USERNAME=$(vault read -field=es_username /secret/performance/employees/cloud/esbench-metrics)
METRICS_PASSWORD=$(vault read -field=es_password /secret/performance/employees/cloud/esbench-metrics)

jq -c '.[]' "benchmarks/build/result.json" | while read -r doc; do
  curl -s -X POST "https://$METRICS_HOST/$METRICS_INDEX_NAME/_doc" \
    -u "$METRICS_USERNAME:$METRICS_PASSWORD" \
    -H 'Content-Type: application/json' \
    -d "$doc"
done
