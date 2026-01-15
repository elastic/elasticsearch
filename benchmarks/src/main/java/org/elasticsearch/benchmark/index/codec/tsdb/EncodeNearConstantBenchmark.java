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
import org.elasticsearch.benchmark.index.codec.tsdb.internal.NearConstantWithOutliersSupplier;
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
 * Benchmark for encoding near-constant data patterns with outliers.
 *
 * <p>Parameterized by outlier probability to test how occasional spikes affect
 * compression. Lower probability means more constant values (better compression),
 * higher probability simulates noisier data.
 */
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class EncodeNearConstantBenchmark {
    private static final int SEED = 17;

    @Param({ "0.0", "0.01", "0.05", "0.1", "0.2" })
    private double outlierProbability;

    private final AbstractTSDBCodecBenchmark encode;
    private NearConstantWithOutliersSupplier supplier;

    public EncodeNearConstantBenchmark() {
        this.encode = new EncodeBenchmark();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        encode.setupInvocation();
    }

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        supplier = NearConstantWithOutliersSupplier.builder(SEED, encode.getBlockSize()).withOutlierProbability(outlierProbability).build();
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
