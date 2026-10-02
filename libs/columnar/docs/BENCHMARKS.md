# Benchmarks

ColumNAR's JMH benchmarks live in the shared `:benchmarks` module, package
`org.elasticsearch.benchmark.index.codec.columnar`.

## End-to-end benchmarks

These drive the full stack through plain Lucene (`IndexWriter`, `IndexSearcher`) and compare
ColumNAR against the numeric codecs it aims to reach parity with. Each takes a `format`
param (`LUCENE`, `ES819`, `ES95`, `COLUMNAR`) and a `blockSize` param.

- `ColumnarNumericIngestBenchmark` — ingest cost and storage. `merge` selects no merges, natural
  merges, or a force-merge; secondary counters report bytes on disk and total bytes written.
- `ColumnarNumericDecodeBenchmark` — full-scan sequential decode throughput. One segment is built
  once per trial and decoded repeatedly; measures the average time per full pass.
- `ColumnarNumericRangeSlicingBenchmark` — range-query latency under
  `DataPartitioning.DOC` slicing.

LUCENE and ES819 ignore `blockSize`; run them once at 128 as a baseline. ES95 and ColumNAR both
support 128 and 512; run them together for an apples-to-apples parity comparison.

```
# Baselines
./gradlew :benchmarks:run --args="ColumnarNumericIngestBenchmark -p format=LUCENE,ES819 -p blockSize=128"
./gradlew :benchmarks:run --args="ColumnarNumericDecodeBenchmark -p format=LUCENE,ES819 -p blockSize=128"
./gradlew :benchmarks:run --args="ColumnarNumericRangeSlicingBenchmark -p format=LUCENE,ES819 -p workload=MONOTONIC_TIMESTAMPS,RANDOM_FULL -p blockSize=128"

# Parity (ES95 vs ColumNAR at matching block sizes)
./gradlew :benchmarks:run --args="ColumnarNumericIngestBenchmark -p format=ES95,COLUMNAR -p blockSize=128,512"
./gradlew :benchmarks:run --args="ColumnarNumericDecodeBenchmark -p format=ES95,COLUMNAR -p blockSize=128,512"
./gradlew :benchmarks:run --args="ColumnarNumericRangeSlicingBenchmark -p format=ES95,COLUMNAR -p workload=MONOTONIC_TIMESTAMPS,RANDOM_FULL -p blockSize=128,512"
```

## Per-stage encode/decode benchmarks

These measure encode and decode throughput for each `BlockTransform` stage in isolation,
without Lucene IO overhead. Use them to freeze per-stage throughput baselines, identify
the dominant stage in a composed pipeline, and catch regressions in a specific encoding.

- `EncodeBlockTransformBenchmark` — encode throughput. `stage` selects the transform
  (`delta`, `offset`, `gcd`, `splitDelta`, `alp`, `for`); `pattern` selects the block shape.
- `DecodeBlockTransformBenchmark` — decode throughput. Same parameters.

Every entry pairs the named transform with a `RawTerminal` (raw 8-byte longs, no bit-packing),
so the throughput score reflects only that stage's work. `for` runs the FOR bit-packer alone
with no preceding transform. The composed pipeline cost is covered by the end-to-end benchmarks.

Block shapes and the stage each one exercises most:

| Pattern | Primary stage |
|---|---|
| `MONOTONIC_TIMESTAMPS` | delta |
| `COUNTER_STEADY` | delta + gcd |
| `GAUGE` | offset |
| `TSDB_SPLIT` | splitDelta |
| `SENSOR_DOUBLES` | alp |
| `RANDOM_FULL` | none (skip baseline) |
| `CONSTANT` | all stages collapse (FOR needs 1 bit; collapse baseline) |
| `DECREASING` | delta (all-negative deltas) |
| `GCD_FRIENDLY` | gcd (random multiples of 1 000 000) |
| `NEAR_CONSTANT_OUTLIERS` | offset (~5% wide outliers over a fixed base) |

```
./gradlew :benchmarks:run --args="EncodeBlockTransformBenchmark"
./gradlew :benchmarks:run --args="DecodeBlockTransformBenchmark"

# Single stage across all patterns
./gradlew :benchmarks:run --args="EncodeBlockTransformBenchmark -p stage=splitDelta"

# Quick smoke
./gradlew :benchmarks:run --args="EncodeBlockTransformBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1 -p stage=delta -p pattern=MONOTONIC_TIMESTAMPS"
```

**Practice:** when adding a new `BlockTransform`, add the stage to both benchmarks. Add a
block shape to `NumericData` only if no existing shape exercises the new stage.

## General

No results are committed — these are for investigation, run on demand.
