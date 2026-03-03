/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.pipeline;

import org.elasticsearch.benchmark.index.codec.tsdb.internal.NumericDataSupplierRegistry;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.ThroughputMetrics;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class EncodeDoubleGeneratorsPipelineBenchmark {

    @Param(
        {
            "constant-double",
            "percentage-double",
            "monotonic-double",
            "gauge-double",
            "realistic-gauge-double",
            "sparse-gauge-double",
            "random-double",
            "stable-sensor-double",
            "tiny-increment-double",
            "steady-counter-double",
            "burst-spike-double",
            "zero-crossing-oscillation-double",
            "step-with-spikes-double",
            "counter-with-resets-double",
            "quantized-double",
            "sensor-2dp-double",
            "temperature-1dp-double",
            "financial-2dp-double",
            "percentage-rounded-1dp-double",
            "mixed-sign-double",
            "step-hold-double",
            "timestamp-as-double",
            "counter-as-double",
            "gauge-as-double",
            "gcd-as-double",
            "constant-as-double",
            "random-as-double",
            "decreasing-timestamp-as-double",
            "small-as-double",
            "timestamp-with-jitter-as-double" }
    )
    private String datasetName;

    @Param({ "42" })
    private String seed;

    @Param({ "128" })
    private int blockSize;

    @Param({ "10" })
    private int blocksPerInvocation;

    @Param(
        {
            "delta-offset-gcd-bitpack",
            "integer-pipeline",
            "alp-double-lossless",
            "alp-double-1e4",
            "alp-double-1e2",
            "fpc-lossless",
            "fpc-1e4",
            "fpc-1e2",
            "gorilla-lossless",
            "gorilla-1e4",
            "gorilla-1e2",
            "chimp-lossless",
            "chimp-1e4",
            "chimp128-1e2" }
    )
    private String pipeline;

    private AbstractPipelineBenchmark encode;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        encode = new PipelineEncodeBenchmark(OptimalPipelines.byName(pipeline, blockSize));
        final Map<String, Supplier<long[]>> registry = NumericDataSupplierRegistry.toMap(
            NumericDataSupplierRegistry.doubleSuppliers(encode.getBlockSize(), Long.parseLong(seed))
        );
        if (registry.size() != 30) {
            throw new IllegalStateException(
                "Expected 30 double datasets but found " + registry.size() + ". Update @Param list. Available: " + registry.keySet()
            );
        }
        final Supplier<long[]> supplier = registry.get(datasetName);
        if (supplier == null) {
            throw new IllegalArgumentException("Unknown double dataset: [" + datasetName + "]. Available: " + registry.keySet());
        }
        encode.setupTrial(supplier);
        encode.setBlocksPerInvocation(blocksPerInvocation);
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        encode.setupIteration();
    }

    @Benchmark
    public void throughput(Blackhole bh, ThroughputMetrics metrics) throws IOException {
        encode.benchmark(bh);
        metrics.recordOperation(encode.getBlockSize() * blocksPerInvocation, encode.getEncodedSize() * blocksPerInvocation);
    }
}
