/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.elasticsearch.benchmark.index.codec.tsdb.internal.AbstractTSDBCodecBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.CounterWithResetsSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.DecodeBenchmark;
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

/**
 * Benchmark for decoding counter-with-resets data patterns.
 *
 * <p>Parameterized by resetProbability to test how reset frequency affects
 * decoding performance. Lower probability means longer monotonic runs between
 * resets, while higher probability creates more frequent jumps back to zero.
 */
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class DecodeCounterWithResetsBenchmark {
    private static final int SEED = 17;

    @Param({ "0.01", "0.02", "0.05" })
    private double resetProbability;

    /**
     * Number of blocks decoded per measured benchmark invocation.
     *
     * <p>Default is 10: the smallest batch size that provides stable measurements with good
     * signal-to-noise ratio for regression tracking. Exposed as a JMH parameter to allow
     * tuning without code changes.
     */
    @Param({ "10" })
    private int blocksPerInvocation;

    private final AbstractTSDBCodecBenchmark decode;

    public DecodeCounterWithResetsBenchmark() {
        this.decode = new DecodeBenchmark();
    }

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        decode.setupTrial(CounterWithResetsSupplier.builder(SEED, decode.getBlockSize()).withResetProbability(resetProbability).build());

        decode.setBlocksPerInvocation(blocksPerInvocation);
        decode.run();
    }

    @Benchmark
    public void throughput(Blackhole bh, ThroughputMetrics metrics) throws IOException {
        decode.benchmark(bh);
        metrics.recordOperation(decode.getBlockSize() * blocksPerInvocation, decode.getEncodedSize() * blocksPerInvocation);
    }
}
