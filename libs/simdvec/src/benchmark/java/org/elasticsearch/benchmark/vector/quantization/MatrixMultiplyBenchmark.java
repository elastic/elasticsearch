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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
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

    /*
     * The m/k/n values are the union of the dimensions ASH multiplies while training a projection
     * matrix, so a regression on any one of them maps onto index-time cost. For a vector dimension D
     * the projected dimension is D/2 (DEFAULT_PROJECTED_DIMS_FRACTION) and the training sample T is
     * min(D * trainingFactor, segment size, MAX_TRAINING_SAMPLES), which caps at 8192. The products
     * are X @ P and X @ B (T x D x D/2), X^T @ W (D x T x D/2), X_ld @ R (T x D/2 x D/2),
     * X_ld^T @ X_enc (D/2 x T x D/2), W = P @ R (D x D/2 x D/2) and the square procrustes product
     * (D/2 cubed), which runs up to 200 times per training iteration and so dominates at low D.
     *
     * Covered here: D of 256, 1024 and 4096, T of 4096 and 8192, plus 3001 for a T below
     * D * trainingFactor and 341 for D/2 at a non-default fraction of 1/3. The last two are odd, so
     * they exercise the loop tails. The cross product also runs combinations ASH never produces.
     */

    /** D/2, D, T, or an odd T from a small segment. */
    @Param({ "128", "341", "512", "1024", "3001", "4096", "8192" })
    int m;

    /** D/2, D, T, or an odd T from a small segment. */
    @Param({ "128", "341", "512", "1024", "3001", "4096", "8192" })
    int k;

    /** Always a projected dimension: D/2 for D of 256, 1024 and 4096, plus an odd 341. */
    @Param({ "128", "341", "512", "2048" })
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
