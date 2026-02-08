/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.pipeline;

import org.elasticsearch.benchmark.index.codec.tsdb.internal.ThroughputMetrics;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.TimestampLikeSupplier;
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
import java.util.concurrent.TimeUnit;

@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class DecodeTimestampLikePipelineBenchmark {
    private static final int SEED = 17;

    @Param({ "0.0", "0.1", "0.2", "0.5" })
    private double jitterProbability;

    @Param({ "10" })
    private int blocksPerInvocation;

    // NOTE: "full" is ES87-compatible; "timestamp" is optimal for this data pattern (skips gcd)
    @Param({ "full", "timestamp" })
    private String pipeline;

    private AbstractPipelineBenchmark decode;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        decode = new PipelineDecodeBenchmark(OptimalPipelines.byName(pipeline));
        TimestampLikeSupplier supplier = TimestampLikeSupplier.builder(SEED, decode.getBlockSize())
            .withJitterProbability(jitterProbability)
            .build();
        decode.setupTrial(supplier);
        decode.setBlocksPerInvocation(blocksPerInvocation);
        decode.setupIteration();
        decode.run();
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
