/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.elasticsearch.benchmark.index.codec.tsdb.internal.AbstractDocValuesForUtilBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.DecodeBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.DecreasingIntegerSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.EncodeBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.IncreasingIntegerSupplier;
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
@Measurement(iterations = 10)
@BenchmarkMode(value = Mode.AverageTime)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@State(value = Scope.Benchmark)
public class ES87TSDBDocValuesIntegerBenchmark {
    private static final int SEED = 17;
    private static final int BLOCK_SIZE = 128;
    @Param({ "4", "8", "12", "16", "24", "28", "32", "36", "40", "44", "48", "52", "56", "64" })
    private int bitsPerValue;
    @Param({ "decode increasing integer", "decode decreasing integer", "encode increasing integer", "encode decreasing integer" })
    private String mode;

    private final AbstractDocValuesForUtilBenchmark decodeIncreasingInteger;
    private final AbstractDocValuesForUtilBenchmark decodeDecreasingInteger;
    private final AbstractDocValuesForUtilBenchmark encodeIncreasingInteger;
    private final AbstractDocValuesForUtilBenchmark encodeDecreasingInteger;

    public ES87TSDBDocValuesIntegerBenchmark() {
        this.decodeIncreasingInteger = new DecodeBenchmark();
        this.decodeDecreasingInteger = new DecodeBenchmark();
        this.encodeIncreasingInteger = new EncodeBenchmark();
        this.encodeDecreasingInteger = new EncodeBenchmark();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        switch (mode) {
            case "decode increasing integer" -> decodeIncreasingInteger.setupInvocation(bitsPerValue);
            case "decode decreasing integer" -> decodeDecreasingInteger.setupInvocation(bitsPerValue);
            case "encode increasing integer" -> encodeIncreasingInteger.setupInvocation(bitsPerValue);
            case "encode decreasing integer" -> encodeDecreasingInteger.setupInvocation(bitsPerValue);
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        switch (mode) {
            case "decode increasing integer" -> decodeIncreasingInteger.setupIteration(
                bitsPerValue,
                new IncreasingIntegerSupplier(SEED, bitsPerValue, BLOCK_SIZE)
            );
            case "decode decreasing integer" -> decodeDecreasingInteger.setupIteration(
                bitsPerValue,
                new DecreasingIntegerSupplier(SEED, bitsPerValue, BLOCK_SIZE)
            );
            case "encode increasing integer" -> encodeIncreasingInteger.setupIteration(
                bitsPerValue,
                new IncreasingIntegerSupplier(SEED, bitsPerValue, BLOCK_SIZE)
            );
            case "encode decreasing integer" -> encodeDecreasingInteger.setupIteration(
                bitsPerValue,
                new DecreasingIntegerSupplier(SEED, bitsPerValue, BLOCK_SIZE)
            );
        }
    }

    @Benchmark
    public void benchmark(Blackhole bh) throws IOException {
        switch (mode) {
            case "decode increasing integer" -> decodeIncreasingInteger.benchmark(bitsPerValue, bh);
            case "decode decreasing integer" -> decodeDecreasingInteger.benchmark(bitsPerValue, bh);
            case "encode increasing integer" -> encodeIncreasingInteger.benchmark(bitsPerValue, bh);
            case "encode decreasing integer" -> encodeDecreasingInteger.benchmark(bitsPerValue, bh);
        }
    }
}
