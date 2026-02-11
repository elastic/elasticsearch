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

// NOTE: Pipeline decoding benchmark using NumericDataGenerators double data sources.
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class DecodeDoubleGeneratorsPipelineBenchmark {

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
            "full",
            "alp_double",
            "alp_rd_double",
            "gorilla",
            "rle-only",
            "xor-bitpack",
            "offset-simplebitpack",
            "delta-simplebitpack",
            "delta-offset-gcd-simplebitpack",
            "delta-bitpack",
            "rle-bitpack",
            "delta-offset-gcd-bitpack",
            "alp_double_stage-offset-gcd-bitpack",
            "alp_double_stage-gcd-bitpack",
            "alp_rd_double_stage-offset-gcd-bitpack",
            "alp_rd_double_stage-gcd-bitpack" }
    )
    private String pipeline;

    private AbstractPipelineBenchmark decode;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        decode = new PipelineDecodeBenchmark(OptimalPipelines.byName(pipeline, blockSize));
        final Map<String, Supplier<long[]>> registry = NumericDataSupplierRegistry.toMap(
            NumericDataSupplierRegistry.doubleSuppliers(decode.getBlockSize(), Long.parseLong(seed))
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
        decode.setupTrial(supplier);
        decode.setBlocksPerInvocation(blocksPerInvocation);
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        decode.setupIteration();
    }

    @Benchmark
    public void throughput(Blackhole bh, ThroughputMetrics metrics) throws IOException {
        decode.benchmark(bh);
        metrics.recordOperation(decode.getBlockSize() * blocksPerInvocation, decode.getEncodedSize() * blocksPerInvocation);
    }
}
