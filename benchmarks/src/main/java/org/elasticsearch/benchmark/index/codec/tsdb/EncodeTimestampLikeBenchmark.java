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

/**
 * Benchmark for encoding timestamp-like data patterns.
 *
 * <p>Parameterized by jitter probability to test how delta encoding handles
 * varying degrees of timestamp regularity. Lower jitter means more consistent
 * deltas (better compression), higher jitter simulates irregular sampling.
 */
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class EncodeTimestampLikeBenchmark {
    private static final int SEED = 17;

    @Param({ "0.0", "0.1", "0.2", "0.5" })
    private double jitterProbability;

    private final AbstractTSDBCodecBenchmark encode;
    private TimestampLikeSupplier supplier;

    public EncodeTimestampLikeBenchmark() {
        this.encode = new EncodeBenchmark();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        encode.setupInvocation();
    }

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        supplier = TimestampLikeSupplier.builder(SEED, encode.getBlockSize()).withJitterProbability(jitterProbability).build();
        encode.setupTrial(supplier);
        encode.setupInvocation();
        encode.run();
    }

    @Benchmark
    public void throughput(Blackhole bh, ThroughputMetrics metrics) throws IOException {
        encode.benchmark(bh);
        metrics.recordOperation(encode.getBlockSize(), encode.getEncodedSize());
    }

    /**
     * Measures compression efficiency metrics (compression ratio, encoded bits/bytes per value).
     *
     * <p>Uses zero warmup and single iteration because compression metrics are deterministic:
     * the same input data always produces the same encoded size. Unlike throughput measurements
     * which vary due to JIT compilation and CPU state, compression ratios are constant across runs.
     */
    @Benchmark
    @Warmup(iterations = 0)
    @Measurement(iterations = 1)
    public void compression(Blackhole bh, CompressionMetrics metrics) throws IOException {
        encode.benchmark(bh);
        metrics.recordOperation(encode.getBlockSize(), encode.getEncodedSize(), supplier.getNominalBitsPerValue());
    }
}
