# Benchmarks

## Running benchmarks

Run from the `benchmarks/` directory using the `run` task with `--args`. Always use the fully-qualified class
name including package to avoid ambiguity. Always pipe through `tee /tmp/bench/<descriptive_name>` using a filename that reflects the task (e.g. `tee /tmp/bench/paged_write`).

```
cd benchmarks
../gradlew run --args "org.elasticsearch.benchmark._nightly.BytesBuilderBenchmark -pdata=1000_ints -pimpl=paged -poperation=write -rf json -rff build/jmh-result.json" | tee /tmp/bench/paged_write
```

## ColumNAR transform benchmarks

```
cd benchmarks
../gradlew run --args="EncodeBlockTransformBenchmark" | tee /tmp/bench/encode_transform
../gradlew run --args="DecodeBlockTransformBenchmark" | tee /tmp/bench/decode_transform

# Single stage + pattern
../gradlew run --args="EncodeBlockTransformBenchmark -p stage=splitDelta -p pattern=TSDB_SPLIT" | tee /tmp/bench/encode_splitdelta_tsdb

# Quick smoke
../gradlew run --args="EncodeBlockTransformBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1 -p stage=delta -p pattern=MONOTONIC_TIMESTAMPS"
```

## Self-test

Never skip the self-test. Do not pass `-DskipSelfTest=true` or `--test` to `run.sh`. The
self-test validates correctness across all impl/operation/data combinations and poisons virtual
dispatch to behave more like production.

## Shard indexing: row path vs columnar batch path

`org.elasticsearch.benchmark.index.mapper.batch.ShardIndexingBenchmark` measures the data-node shard
stage of indexing on identical documents through four paths:

- `column`: `ShardBatchMapper.mapColumnBatch` then `IndexWriter.addBatch`, chunked at
  `ShardBatchIndexer.BATCH_CHUNK_SIZE`. This is the columnar batch path.
- `rowBatch`: the production fallback when a batch cannot go columnar: `RowSource` over the batch
  row through `IndexShard.prepareIndex` and `addDocuments`.
- `rowJson`: classic `_bulk`, JSON bytes through `prepareIndex`.
- `columnThenRow`: what production pays for a shape that falls back: `resolveMappers`, the doomed
  `mapColumnBatch`, then `rowBatch`.

### Recipes

- Regression check on a mapper change: run `column` on the shapes that touch that mapper.
  Allocation first, `-f 1 -wi 2 -w 1 -i 2 -r 1 -prof gc`, base vs branch; any B/doc delta beyond a
  few bytes is real. Then timing with the defaults; if the delta is under 5%, interleave base and
  branch forks (A B A B) and compare the median of per-fork means.
- Is the columnar path worth it for a shape: `column` against `rowBatch` on the same shape, read
  the docs/s ratio and B/doc. `rowJson` is the classic `_bulk` reference, not the fallback.
- Where did the cost go: `-prof async` on one shape in a separate run (see `README.md`); the
  collapsed stacks split into `ShardBatchMapper.mapColumnBatch` and `IndexingChain.processBatch`.

### What is measured

The timed region is mapping plus `addBatch` (or `addDocuments`) only. Nothing in the benchmark
flushes: `deleteAll` runs in the per-invocation teardown and `commit` in the per-iteration
teardown. Both are outside the timed region but inside the `gc.alloc.rate.norm` window, because
JMH's GC profiler snapshots allocation per iteration, so B/doc includes a small constant of writer
bookkeeping. B/doc is comparable across methods and shapes, but it is not purely mapper allocation.
Do not add a flush to the timed methods: a 5000-doc segment costs one to two milliseconds of file
creation and compound packing on macOS, which was 80% of the timed op and hid mapper changes.

The engine is bypassed. Version map, translog, `_id` lookup and `IndexResult` handling are not
measured. Codec work is not measured either; a change that alters what the mappers hand to Lucene
shows up in `ShardIndexingBenchmarkTests` (field infos and per-document values must match between
row and column paths), not in the numbers.

### Shapes

Inputs are named `Shapes.BenchShape` entries in `Shapes` (mapping, document generator, features,
expected path). Select with `-p shape=a,b,c`. Defaults are a handful per state; the rest of the
catalog is in `@ExtraParam` and in `Shapes.names()`. Each state only accepts shapes with the right
`Shapes.Expect`: `ColumnState` needs `COLUMN`, `FallbackState` needs `FALLBACK_RESOLVE` or
`FALLBACK_MAP`, `RowState` takes anything. For `ColumnState` and `FallbackState`, setup dry-runs
every request through `ColumnIndexer.dryRunStage` and fails loudly when the stage it reaches is not
the shape's `Expect`, so a fallback can never be measured as a fast column result.

Facts about the code the catalog pins down:

- Arrays mixing float-exact and non-float-exact values (`[50.5, 50.4]`) encode as UNION and fall
  back at `mapColumnBatch`; the floating `mv1to4` shapes are therefore fallback shapes.
- Explicit JSON nulls make a UNION column and fall back; absent values ride the validity bitset.
- `doc_values=false` is rejected by columnar mode at mapping time; `scaled_float` is the only
  resolve-level fallback shape.
- The JSON token kind, not the mapping type, selects the transform: integers into a `long` field
  alias the input column, doubles or numeric strings allocate.

Absent values and arrays both force the column off the dense fast path in `IndexingChain`, which
is where most of their cost sits, not in the mapper.

`docsPerRequest` splits the 5000 docs per invocation into requests. Sizes above
`BATCH_CHUNK_SIZE` belong in a whole-request benchmark, not here.

### Reading the numbers

- docs/s covers the timed region; `gc.alloc.rate.norm` (bytes per doc) also includes the teardowns
  (see above).
- `pagesObtained` and `pagesFresh` are aux counters for recycler pages. Pooled pages are invisible
  to the allocation profiler, so a change in page count with flat B/doc is still a change.
  Pages per doc is the counter divided by docs/s. Aliased long columns obtain no pages.
- Column B/doc is identical across forks and converged by the second iteration. Row B/doc can
  differ about 4% between forks from C2 escape-analysis decisions, so compare it across forks.
- Warmup: the first iteration runs 20 to 30% below steady state and the column path keeps drifting
  up for 10 to 25 seconds, hence 10 warmup iterations.
- Iterations: within a fork the interval is under 3% after 3 to 4 iterations.
- Forks: the spread between forks is 4 to 6% and is the dominant noise. 5 forks resolve a 3%
  delta, 3 do not. Below 5%, compare interleaved forks of base and branch (A B A B) on the median
  of per-fork means, and run an A/A first to find the floor on the machine that day.
- Calibrated 2026-10-13 on an M3 Pro with `-f 5 -wi 0 -i 60 -r 1` on `long.dense.small`.

### Running

```
./gradlew -p benchmarks run --args 'ShardIndexingBenchmark\.column$ -prof gc -p shape=long.dense.small,long.mv1to4'
./gradlew -p benchmarks run --args 'ShardIndexingBenchmark\.(column|rowBatch)$ -prof gc -p shape=metrics4 -p docsPerRequest=100'
./gradlew :benchmarks:test --tests '*ShardIndexingBenchmarkTests*'
```

Add `-f 1 -wi 2 -w 1 -i 2 -r 1` for a quick allocation check on the column path; keep the defaults
(5 forks, 10 + 5 one-second iterations, 75 s per cell) for timing.

### Adding a shape

Add a `Shapes.numeric(...)` builder call (or a new family) to the catalog, set `expect` to what the
code actually does, and add the name to the matching state's `@Param` or `@ExtraParam` list.
`ShardIndexingBenchmarkTests.testParamsAreKnownShapes` fails until the lists and the catalog agree.

A new family (keyword, date, ...) needs its own generator and mapping fragment but the same
fixture. Keep building batches through `IndexingFixture`'s `BulkShardBatch` round trip: ESCF long
columns are not 8-byte aligned, so `EscfBatch.parse` over a paged `BytesReference` asserts under
`-ea` and silently drops off the columnar path without it.
