/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.pipeline;

import org.elasticsearch.benchmark.index.codec.tsdb.internal.CompressionMetrics;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.NonSortedIntegerSupplier;
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
import java.util.concurrent.TimeUnit;

@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class EncodeNonSortedIntegerPipelineBenchmark {
    private static final int SEED = 17;

    @Param({ "8", "16", "24", "32", "48", "64" })
    private int bitsPerValue;

    @Param({ "10" })
    private int blocksPerInvocation;

    // NOTE: "full" is ES87-compatible; "nonSorted" is optimal for this data pattern (skips delta, gcd)
    @Param({ "full", "nonSorted" })
    private String pipeline;

    private AbstractPipelineBenchmark encode;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        encode = new PipelineEncodeBenchmark(OptimalPipelines.byName(pipeline));
        NonSortedIntegerSupplier supplier = new NonSortedIntegerSupplier(SEED, bitsPerValue, encode.getBlockSize());
        encode.setupTrial(supplier);
        encode.setBlocksPerInvocation(blocksPerInvocation);
        encode.setupIteration();
        encode.run();
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

    @Benchmark
    @Warmup(iterations = 0)
    @Measurement(iterations = 1)
    @BenchmarkMode(Mode.SingleShotTime)
    public void compression(Blackhole bh, CompressionMetrics metrics) throws IOException {
        encode.benchmark(bh);
        metrics.recordOperation(encode.getBlockSize(), encode.getEncodedSize(), bitsPerValue);
    }
}
