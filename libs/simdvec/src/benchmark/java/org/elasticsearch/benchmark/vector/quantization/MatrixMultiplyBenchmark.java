/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.quantization;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.benchmark.vector.VectorImplementation;
import org.elasticsearch.benchmark.vector.VectorizationInfo;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.simdvec.ESVectorizationProvider;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
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

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class MatrixMultiplyBenchmark {

    static {
        BenchmarkLogging.configure();
        VectorizationInfo.printOnce();
    }

    @Param({ "SCALAR", "PANAMA" })
    VectorImplementation implementation;

    // ASH defaults are 10240 x 1024 x 512
    // also use an odd number to exercise the tails
    @Param({ "192", "481", "768", "10240" })
    int m;

    @Param({ "192", "481", "768", "1024" })
    int k;

    @Param({ "96", "241", "384", "512" })
    int n;

    private ESVectorUtilSupport impl;
    /** A is (m x k). */
    private float[] a;
    /** B for matrixMultiply: (k x n). */
    private float[] bMul;
    private float[] result;

    @Setup(Level.Trial)
    public void init() {
        impl = switch (implementation) {
            case SCALAR -> ESVectorizationProvider.lookup(false, false).getVectorUtilSupport();
            case PANAMA -> ESVectorizationProvider.lookup(true, false).getVectorUtilSupport();
            default -> throw new AssertionError(implementation);
        };
        Random random = new Random();
        a = VectorTestUtils.randomFloatVector(random, m * k);
        bMul = VectorTestUtils.randomFloatVector(random, k * n);
        result = new float[m * n];
    }

    @Setup(Level.Iteration)
    public void reset() {
        Arrays.fill(result, 0);
    }

    /** C = A @ B, A is (m x k), B is (k x n), C is (m x n). */
    @Benchmark
    public void matrixMultiply(Blackhole bh) {
        impl.matrixMultiply(a, bMul, m, k, n, result);
        bh.consume(result);
    }
}
