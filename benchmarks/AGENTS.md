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
