#!/bin/bash

jq -c '.[]' "benchmarks/build/result.json" | while read -r doc; do
  echo "Indexing $(echo "$doc" | jq -r '.benchmark')"
  doc = $doc | jq --arg timestamp "$(date +%s000)" '. + {"@timestamp": $timestamp}'
  curl -s -X POST "https://$PERF_METRICS_HOST/$PERF_METRICS_INDEX/_doc" \
    -u "$PERF_METRICS_USERNAME:$PERF_METRICS_PASSWORD" \
    -H 'Content-Type: application/json' \
    -d "$doc"
done
