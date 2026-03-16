/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.benchmark.Utils;
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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.packNibbles;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.randomInt4Bytes;

/**
 * Benchmark comparing raw int4 packed-nibble dot product implementations:
 * scalar (plain loop) vs Lucene (Panama-vectorized VectorUtil).
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerInt4OperationBenchmark'
 */
@Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class VectorScorerInt4OperationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    public byte[] unpacked;
    public byte[] packed;

    @Param({ "2", "128", "208", "256", "300", "512", "702", "1024", "1536", "2048" })
    public int size;

    @Setup(Level.Iteration)
    public void init() {
        unpacked = new byte[size];
        randomInt4Bytes(ThreadLocalRandom.current(), unpacked);
        packed = packNibbles(unpacked);
    }

    @Benchmark
    public int scalar() {
        return ScalarOperations.dotProductI4SinglePacked(unpacked, packed);
    }

    @Benchmark
    public int lucene() {
        return VectorUtil.int4DotProductSinglePacked(unpacked, packed);
    }
}
