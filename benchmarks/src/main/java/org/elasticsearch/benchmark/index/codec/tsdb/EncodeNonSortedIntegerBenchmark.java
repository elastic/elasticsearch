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
import org.elasticsearch.benchmark.index.codec.tsdb.internal.CompressionMetrics;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.EncodeBenchmark;
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

/**
 * Benchmark for encoding non-sorted integer patterns.
 */
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class EncodeNonSortedIntegerBenchmark {
    private static final int SEED = 17;

    @Param({ "1", "4", "8", "9", "16", "17", "24", "25", "32", "33", "40", "48", "56", "57", "64" })
    private int bitsPerValue;

    /**
     * Number of blocks encoded per measured benchmark invocation.
     *
     * <p>Default is 10: the smallest batch size that provides stable measurements with good
     * signal-to-noise ratio for regression tracking. Exposed as a JMH parameter to allow
     * tuning without code changes.
     */
    @Param({ "10" })
    private int blocksPerInvocation;

    private final AbstractTSDBCodecBenchmark encode;

    public EncodeNonSortedIntegerBenchmark() {
        this.encode = new EncodeBenchmark();
    }

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        encode.setupTrial(new NonSortedIntegerSupplier(SEED, bitsPerValue, encode.getBlockSize()));

        encode.setBlocksPerInvocation(blocksPerInvocation);
        encode.run();
    }

    @Benchmark
    public void throughput(Blackhole bh, ThroughputMetrics metrics) throws IOException {
        encode.benchmark(bh);
        metrics.recordOperation(encode.getBlockSize() * blocksPerInvocation, encode.getEncodedSize() * blocksPerInvocation);
    }

    /**
     * Reports compression metrics (encoded size, compression ratio, bits per value).
     *
     * <p>This benchmark exists only to collect compression statistics via {@link CompressionMetrics}.
     * The reported time is not meaningful and should be ignored. Metrics are per-block regardless
     * of the {@code blocksPerInvocation} setting.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 0)
    @Measurement(iterations = 1)
    public void compression(Blackhole bh, CompressionMetrics metrics) throws IOException {
        encode.benchmark(bh);
        metrics.recordOperation(encode.getBlockSize(), encode.getEncodedSize(), bitsPerValue);
    }
}
