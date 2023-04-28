/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.elasticsearch.benchmark.index.codec.tsdb.internal.AbstractDocValuesForUtilBenchmark;
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
public class EncodeIncreasingIntegerBenchmark {
    private static final int SEED = 17;
    private static final int BLOCK_SIZE = 128;
    @Param({ "4", "8", "12", "16", "20", "24", "28", "32", "36", "40", "44", "48", "52", "56", "60", "64" })
    private int bitsPerValue;

    private final AbstractDocValuesForUtilBenchmark encode;

    public EncodeIncreasingIntegerBenchmark() {
        this.encode = new EncodeBenchmark();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        encode.setupInvocation(bitsPerValue);
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        encode.setupIteration(bitsPerValue, new IncreasingIntegerSupplier(SEED, bitsPerValue, BLOCK_SIZE));
    }

    @Benchmark
    public void benchmark(Blackhole bh) throws IOException {
        encode.benchmark(bitsPerValue, bh);
    }
}
