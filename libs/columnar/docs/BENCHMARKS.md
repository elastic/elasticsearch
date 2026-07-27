# Benchmarks

ColumNAR's JMH benchmarks live in the shared `:benchmarks` module, package
`org.elasticsearch.benchmark.index.codec.columnar`, so they can compare ColumNAR
against the numeric codecs it aims to reach parity with. Each takes a `format`
param (`COLUMNAR`, `ES819`, `ES95`) and drives all formats over identical data
through plain Lucene (`IndexWriter`, `IndexSearcher`).

- `ColumnarNumericIngestBenchmark` — ingest cost and storage. `merge` selects no merges, natural
  merges, or a force-merge; secondary counters report bytes on disk and total bytes written.
- `ColumnarNumericRangeSlicingBenchmark` — range-query latency under
  `DataPartitioning.DOC` slicing.

Run (JMH args after the class regex):

```
./gradlew :benchmarks:run --args="ColumnarNumericRangeSlicingBenchmark -p format=COLUMNAR,ES819 -p workload=RANDOM_FULL"
./gradlew :benchmarks:run --args="ColumnarNumericIngestBenchmark -p format=COLUMNAR,ES819,ES95"
```

No results are committed — these are for investigation, run on demand.
