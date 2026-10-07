/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.blas;

import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class BlasTests extends ESTestCase {

    static Blas blas;

    @BeforeClass
    public static void getBlas() {
        assumeTrue("Native OpenBLAS library is not available on this platform/JDK", Blas.isSupported());
        var instance = Blas.instance();
        assertTrue("Blas.isSupported() returned true but instance() is empty", instance.isPresent());
        blas = instance.get();
    }

    // --- sgemm ---

    /** C = A * B (no transpose), against a reference triple-loop. */
    public void testSgemmNoTranspose() {
        int m = randomIntBetween(1, 128);
        int k = randomIntBetween(1, 128);
        int n = randomIntBetween(1, 128);
        float[] a = randomFloats(m * k);
        float[] b = randomFloats(k * n);
        float[] expected = refSgemm(false, false, m, n, k, 1f, a, b, 0f, new float[m * n]);
        float[] actual = new float[m * n];
        blas.sgemm(false, false, m, n, k, 1f, a, b, 0f, actual);
        assertArrayEquals(expected, actual, relTol(expected));
    }

    /** C = A^T * B */
    public void testSgemmTransposeA() {
        int m = randomIntBetween(1, 64);
        int k = randomIntBetween(1, 64);
        int n = randomIntBetween(1, 64);
        // A stored as (k x m) in memory (transposed to m x k logically)
        float[] a = randomFloats(k * m);
        float[] b = randomFloats(k * n);
        float[] expected = refSgemm(true, false, m, n, k, 1f, a, b, 0f, new float[m * n]);
        float[] actual = new float[m * n];
        blas.sgemm(true, false, m, n, k, 1f, a, b, 0f, actual);
        assertArrayEquals(expected, actual, relTol(expected));
    }

    /** C = A * B^T */
    public void testSgemmTransposeB() {
        int m = randomIntBetween(1, 64);
        int k = randomIntBetween(1, 64);
        int n = randomIntBetween(1, 64);
        float[] a = randomFloats(m * k);
        // B stored as (n x k) in memory (transposed to k x n logically)
        float[] b = randomFloats(n * k);
        float[] expected = refSgemm(false, true, m, n, k, 1f, a, b, 0f, new float[m * n]);
        float[] actual = new float[m * n];
        blas.sgemm(false, true, m, n, k, 1f, a, b, 0f, actual);
        assertArrayEquals(expected, actual, relTol(expected));
    }

    /** C = A^T * B^T */
    public void testSgemmTransposeBoth() {
        int m = randomIntBetween(1, 64);
        int k = randomIntBetween(1, 64);
        int n = randomIntBetween(1, 64);
        float[] a = randomFloats(k * m);
        float[] b = randomFloats(n * k);
        float[] expected = refSgemm(true, true, m, n, k, 1f, a, b, 0f, new float[m * n]);
        float[] actual = new float[m * n];
        blas.sgemm(true, true, m, n, k, 1f, a, b, 0f, actual);
        assertArrayEquals(expected, actual, relTol(expected));
    }

    /** alpha and beta scaling factors are applied correctly. */
    public void testSgemmScaling() {
        int m = randomIntBetween(2, 32);
        int k = randomIntBetween(2, 32);
        int n = randomIntBetween(2, 32);
        float alpha = randomFloat() * 4 - 2;
        float beta = randomFloat() * 4 - 2;
        float[] a = randomFloats(m * k);
        float[] b = randomFloats(k * n);
        float[] cInit = randomFloats(m * n);
        float[] expected = refSgemm(false, false, m, n, k, alpha, a, b, beta, cInit.clone());
        float[] actual = cInit.clone();
        blas.sgemm(false, false, m, n, k, alpha, a, b, beta, actual);
        assertArrayEquals(expected, actual, relTol(expected));
    }

    /** sgemm operates correctly on native off-heap segments. */
    public void testSgemmNativeSegments() {
        int m = 8, k = 4, n = 6;
        float[] a = randomFloats(m * k);
        float[] b = randomFloats(k * n);
        float[] expected = refSgemm(false, false, m, n, k, 1f, a, b, 0f, new float[m * n]);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segA = arena.allocate((long) m * k * JAVA_FLOAT.byteSize());
            MemorySegment segB = arena.allocate((long) k * n * JAVA_FLOAT.byteSize());
            MemorySegment segC = arena.allocate((long) m * n * JAVA_FLOAT.byteSize());
            MemorySegment.copy(a, 0, segA, JAVA_FLOAT, 0, a.length);
            MemorySegment.copy(b, 0, segB, JAVA_FLOAT, 0, b.length);
            blas.sgemm(false, false, m, n, k, 1f, segA, segB, 0f, segC);
            float[] actual = new float[m * n];
            MemorySegment.copy(segC, JAVA_FLOAT, 0, actual, 0, actual.length);
            assertArrayEquals(expected, actual, relTol(expected));
        }
    }

    // --- sgemv ---

    /** y = A * x (no transpose). */
    public void testSgemvNoTranspose() {
        int rows = randomIntBetween(1, 128);
        int cols = randomIntBetween(1, 128);
        float[] a = randomFloats(rows * cols);
        float[] x = randomFloats(cols);
        float[] expected = refSgemv(false, rows, cols, 1f, a, x, 0f, new float[rows]);
        float[] actual = new float[rows];
        blas.sgemv(false, rows, cols, 1f, a, x, 0f, actual);
        assertArrayEquals(expected, actual, relTol(expected));
    }

    /** y = A^T * x (transpose). */
    public void testSgemvTranspose() {
        int rows = randomIntBetween(1, 64);
        int cols = randomIntBetween(1, 64);
        float[] a = randomFloats(rows * cols);
        float[] x = randomFloats(rows);
        float[] expected = refSgemv(true, rows, cols, 1f, a, x, 0f, new float[cols]);
        float[] actual = new float[cols];
        blas.sgemv(true, rows, cols, 1f, a, x, 0f, actual);
        assertArrayEquals(expected, actual, relTol(expected));
    }

    /** alpha and beta scaling factors are applied correctly by sgemv. */
    public void testSgemvScaling() {
        int rows = randomIntBetween(2, 32);
        int cols = randomIntBetween(2, 32);
        float alpha = randomFloat() * 4 - 2;
        float beta = randomFloat() * 4 - 2;
        float[] a = randomFloats(rows * cols);
        float[] x = randomFloats(cols);
        float[] yInit = randomFloats(rows);
        float[] expected = refSgemv(false, rows, cols, alpha, a, x, beta, yInit.clone());
        float[] actual = yInit.clone();
        blas.sgemv(false, rows, cols, alpha, a, x, beta, actual);
        assertArrayEquals(expected, actual, relTol(expected));
    }

    /** sgemv operates correctly on native off-heap segments. */
    public void testSgemvNativeSegments() {
        int rows = 6, cols = 4;
        float[] a = randomFloats(rows * cols);
        float[] x = randomFloats(cols);
        float[] expected = refSgemv(false, rows, cols, 1f, a, x, 0f, new float[rows]);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segA = arena.allocate((long) rows * cols * JAVA_FLOAT.byteSize());
            MemorySegment segX = arena.allocate((long) cols * JAVA_FLOAT.byteSize());
            MemorySegment segY = arena.allocate((long) rows * JAVA_FLOAT.byteSize());
            MemorySegment.copy(a, 0, segA, JAVA_FLOAT, 0, a.length);
            MemorySegment.copy(x, 0, segX, JAVA_FLOAT, 0, x.length);
            blas.sgemv(false, rows, cols, 1f, segA, segX, 0f, segY);
            float[] actual = new float[rows];
            MemorySegment.copy(segY, JAVA_FLOAT, 0, actual, 0, actual.length);
            assertArrayEquals(expected, actual, relTol(expected));
        }
    }

    // --- validation ---

    public void testSgemmRejectsNonPositiveDimensions() {
        float[] dummy = new float[4];
        var e1 = expectThrows(IllegalArgumentException.class, () -> blas.sgemm(false, false, 0, 1, 1, 1f, dummy, dummy, 0f, dummy));
        assertThat(e1.getMessage(), containsString("m must be positive"));
        var e2 = expectThrows(IllegalArgumentException.class, () -> blas.sgemm(false, false, 1, 0, 1, 1f, dummy, dummy, 0f, dummy));
        assertThat(e2.getMessage(), containsString("n must be positive"));
        var e3 = expectThrows(IllegalArgumentException.class, () -> blas.sgemm(false, false, 1, 1, 0, 1f, dummy, dummy, 0f, dummy));
        assertThat(e3.getMessage(), containsString("k must be positive"));
    }

    public void testSgemmRejectsUndersizedSegments() {
        try (Arena arena = Arena.ofConfined()) {
            // m=2, n=2, k=2: A needs 4 floats, B needs 4 floats, C needs 4 floats.
            MemorySegment small = arena.allocate(JAVA_FLOAT.byteSize());
            MemorySegment ok = arena.allocate(4 * JAVA_FLOAT.byteSize());
            var e = expectThrows(IllegalArgumentException.class, () -> blas.sgemm(false, false, 2, 2, 2, 1f, small, ok, 0f, ok));
            assertThat(e.getMessage(), containsString("'a'"));
        }
    }

    public void testSgemvRejectsNonPositiveDimensions() {
        float[] dummy = new float[4];
        var e1 = expectThrows(IllegalArgumentException.class, () -> blas.sgemv(false, 0, 1, 1f, dummy, dummy, 0f, dummy));
        assertThat(e1.getMessage(), containsString("rows must be positive"));
        var e2 = expectThrows(IllegalArgumentException.class, () -> blas.sgemv(false, 1, 0, 1f, dummy, dummy, 0f, dummy));
        assertThat(e2.getMessage(), containsString("cols must be positive"));
    }

    public void testSgemvRejectsUndersizedSegments() {
        try (Arena arena = Arena.ofConfined()) {
            // rows=2, cols=2: A needs 4 floats, x needs 2 floats, y needs 2 floats.
            MemorySegment small = arena.allocate(JAVA_FLOAT.byteSize());
            MemorySegment ok = arena.allocate(4 * JAVA_FLOAT.byteSize());
            var e = expectThrows(IllegalArgumentException.class, () -> blas.sgemv(false, 2, 2, 1f, small, ok, 0f, ok));
            assertThat(e.getMessage(), containsString("'a'"));
        }
    }

    public void testIsSupported() {
        // When we reach here Blas.isSupported() returned true (see @BeforeClass).
        assertThat(Blas.isSupported(), equalTo(true));
    }

    // --- reference implementations ---

    /**
     * Reference sgemm: {@code C = alpha * op(A) * op(B) + beta * C} using a triple loop.
     * A is stored as m*k when not transposed (k*m when transposed). B is stored as k*n (n*k transposed).
     */
    private static float[] refSgemm(
        boolean transA,
        boolean transB,
        int m,
        int n,
        int k,
        float alpha,
        float[] a,
        float[] b,
        float beta,
        float[] c
    ) {
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                float sum = 0f;
                for (int l = 0; l < k; l++) {
                    float aVal = transA ? a[l * m + i] : a[i * k + l];
                    float bVal = transB ? b[j * k + l] : b[l * n + j];
                    sum = Math.fma(aVal, bVal, sum);
                }
                c[i * n + j] = Math.fma(alpha, sum, beta * c[i * n + j]);
            }
        }
        return c;
    }

    /**
     * Reference sgemv: {@code y = alpha * op(A) * x + beta * y} using a double loop.
     * A is stored as rows*cols in row-major order.
     */
    private static float[] refSgemv(boolean trans, int rows, int cols, float alpha, float[] a, float[] x, float beta, float[] y) {
        int outLen = trans ? cols : rows;
        int inLen = trans ? rows : cols;
        for (int i = 0; i < outLen; i++) {
            float sum = 0f;
            for (int j = 0; j < inLen; j++) {
                float aVal = trans ? a[j * cols + i] : a[i * cols + j];
                sum = Math.fma(aVal, x[j], sum);
            }
            y[i] = Math.fma(alpha, sum, beta * y[i]);
        }
        return y;
    }

    private float[] randomFloats(int len) {
        float[] arr = new float[len];
        for (int i = 0; i < len; i++) {
            arr[i] = (randomFloat() - 0.5f) * 2;
        }
        return arr;
    }

    /**
     * Relative tolerance: 1e-4 times the max-absolute value in the expected result, but at least 1e-6.
     * This adapts to result magnitude, which varies widely with matrix size and alpha.
     */
    private static float relTol(float[] expected) {
        float maxAbs = 0f;
        for (float v : expected) {
            maxAbs = Math.max(maxAbs, Math.abs(v));
        }
        return Math.max(1e-6f, maxAbs * 1e-4f);
    }
}
