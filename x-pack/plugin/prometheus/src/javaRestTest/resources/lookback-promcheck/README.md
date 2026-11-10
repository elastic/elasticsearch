# Selector lookback differential tests

These inputs compare independent lookback windows against Prometheus through PromCheck.
Times in queries.jsonl are offsets in seconds from the newest dataset sample.

From the Elasticsearch checkout root, with the Go PromCheck binary on PATH
(verified with 0.9.5+prometheus-3.13.1):

```sh
tar -czf /tmp/lookback-promcheck.tar.gz -C x-pack/plugin/prometheus/src/javaRestTest/resources/lookback-promcheck dataset.csv queries.jsonl
promcheck --elasticsearch "$PWD" --bias-tol 0 < /tmp/lookback-promcheck.tar.gz
```

The cases cover short and long steps, custom lookback, non-positive request values,
expiry boundaries, aggregation, explicit ranges, offsets, sample timestamps, sub-millisecond values,
sample replacement, independently expiring series
and missing-data fallback.
