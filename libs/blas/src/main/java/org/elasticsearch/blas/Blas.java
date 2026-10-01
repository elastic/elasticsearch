/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.blas;

import org.elasticsearch.foreign.LibraryProvider;
import org.elasticsearch.foreign.Platform;

import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT;

/**
 * Public wrapper for the OpenBLAS single-precision CBLAS routines.
 *
 * <p>Obtain the singleton via {@link #instance()}; it is empty when the native library is
 * unavailable (JDK older than 22, unsupported platform, or disabled via the system property
 * {@code org.elasticsearch.blas.enabled=false}).
 *
 * <p>All matrix arguments follow row-major, contiguous storage without padding — the leading
 * dimension is always equal to the number of columns in the logical (pre-transpose) matrix.
 * The wrapper derives {@code lda}/{@code ldb}/{@code ldc} from the dimension arguments and
 * validates segment sizes before calling into native code, because an out-of-bounds read in
 * native code crashes the JVM.
 *
 * <p><b>Safepoint warning.</b> The underlying {@link BlasLibrary} bindings are
 * {@code @Critical} downcalls. A call holds the executing thread in native state for its
 * entire duration, stalling JVM safepoints (including GC) for that thread until the call
 * returns. Long matrix multiplications (hundreds of milliseconds) delay all safepoints for
 * that thread. Callers should be aware of this when sizing workloads.
 */
public final class Blas {

    /* ref: https://github.com/OpenMathLib/OpenBLAS/blob/develop/cblas.h */
    /** CBLAS row-major order constant ({@code CblasRowMajor = 101}). */
    static final int ROW_MAJOR = 101;
    /** CBLAS no-transpose constant ({@code CblasNoTrans = 111}). */
    static final int NO_TRANS = 111;
    /** CBLAS transpose constant ({@code CblasTrans = 112}). */
    static final int TRANS = 112;

    private static final String ENABLE_PROPERTY = "org.elasticsearch.blas.enabled";

    private static final Optional<Blas> INSTANCE = load();

    /**
     * Returns the native OpenBLAS wrapper, or empty when the native library cannot be used on
     * this JDK/platform combination or is disabled via {@code -Dorg.elasticsearch.blas.enabled=false}.
     */
    public static Optional<Blas> instance() {
        return INSTANCE;
    }

    private static Optional<Blas> load() {
        if (isSupported() == false) {
            return Optional.empty();
        }
        BlasLibrary lib = LibraryProvider.lookupLibrary(BlasLibrary.class);
        if (lib == null) {
            return Optional.empty();
        }
        return Optional.of(new Blas(lib));
    }

    /**
     * Returns {@code true} when the combination of JDK version, platform, and system properties
     * allows the native library to be loaded.
     *
     * <p>The critical-downcall linker option that allows on-heap {@link MemorySegment} arguments
     * without copying requires JDK 22 or later. On older JDKs the binding throws
     * {@link AssertionError} on any invocation (see {@link org.elasticsearch.foreign.Critical.UnsupportedFallback}).
     */
    public static boolean isSupported() {
        if (Runtime.version().feature() < 22) {
            return false;
        }
        // The cross-toolchain darwin sysroot lacks headers OpenBLAS needs (sys/shm.h, sys/ipc.h,
        // sys/sysctl.h, sys/time.h), so no darwin binary is built.
        if (Platform.current() == Platform.DARWIN_X64 || Platform.current() == Platform.DARWIN_AARCH64) {
            return false;
        }
        String prop = System.getProperty(ENABLE_PROPERTY, "true");
        if (prop.equalsIgnoreCase("false")) {
            return false;
        }
        return true;
    }

    private final BlasLibrary lib;

    private Blas(BlasLibrary lib) {
        this.lib = lib;
    }

    /**
     * Single-precision general matrix multiply: {@code C = alpha * op(A) * op(B) + beta * C}.
     *
     * <p>All matrices use row-major, contiguous storage. The leading dimension of each matrix
     * equals the number of columns in its logical (un-transposed) layout.
     *
     * <p>Matrix A is {@code m x k} (or {@code k x m} when {@code transA}); B is {@code k x n}
     * (or {@code n x k} when {@code transB}); C is {@code m x n}.
     *
     * @param transA {@code true} to transpose A before multiplying
     * @param transB {@code true} to transpose B before multiplying
     * @param m      rows of op(A) and C
     * @param n      columns of op(B) and C
     * @param k      columns of op(A) / rows of op(B)
     * @param alpha  scalar multiplier for op(A)*op(B)
     * @param a      matrix A; must hold at least {@code m*k} floats
     * @param b      matrix B; must hold at least {@code k*n} floats
     * @param beta   scalar multiplier for the initial C content
     * @param c      matrix C (in/out); must hold at least {@code m*n} floats
     */
    public void sgemm(
        boolean transA,
        boolean transB,
        int m,
        int n,
        int k,
        float alpha,
        MemorySegment a,
        MemorySegment b,
        float beta,
        MemorySegment c
    ) {
        validatePositive("m", m);
        validatePositive("n", n);
        validatePositive("k", k);
        // Row-major A: logical shape (m x k) without transpose, (k x m) with transpose.
        // The leading dimension (row stride) is k when untransposed, m when transposed.
        int ldA = transA ? m : k;
        int ldB = transB ? k : n;
        int ldC = n;
        validateFloatSegment("a", a, (long) m * k);
        validateFloatSegment("b", b, (long) k * n);
        validateFloatSegment("c", c, (long) m * n);
        lib.sgemm(ROW_MAJOR, transA ? TRANS : NO_TRANS, transB ? TRANS : NO_TRANS, m, n, k, alpha, a, ldA, b, ldB, beta, c, ldC);
    }

    /**
     * Single-precision general matrix multiply with {@code float[]} inputs. Elements are
     * wrapped as heap {@link MemorySegment}s and passed via the critical downcall without
     * copying (requires JDK 22+).
     *
     * @see #sgemm(boolean, boolean, int, int, int, float, MemorySegment, MemorySegment, float, MemorySegment)
     */
    public void sgemm(boolean transA, boolean transB, int m, int n, int k, float alpha, float[] a, float[] b, float beta, float[] c) {
        sgemm(transA, transB, m, n, k, alpha, MemorySegment.ofArray(a), MemorySegment.ofArray(b), beta, MemorySegment.ofArray(c));
    }

    /**
     * Single-precision general matrix-vector multiply: {@code y = alpha * op(A) * x + beta * y}.
     *
     * <p>A uses row-major, contiguous storage. Its leading dimension equals {@code cols}.
     * The vector {@code x} must hold {@code cols} floats (un-transposed) or {@code rows} floats
     * (transposed); {@code y} must hold {@code rows} floats (un-transposed) or {@code cols} floats.
     *
     * @param trans {@code true} to transpose A before multiplying
     * @param rows  number of rows in A
     * @param cols  number of columns in A
     * @param alpha scalar multiplier for op(A)*x
     * @param a     matrix A; must hold at least {@code rows*cols} floats
     * @param x     input vector; must hold at least {@code cols} floats (un-transposed) or {@code rows} (transposed)
     * @param beta  scalar multiplier for the initial y content
     * @param y     output vector (in/out); must hold at least {@code rows} floats (un-transposed) or {@code cols}
     */
    public void sgemv(boolean trans, int rows, int cols, float alpha, MemorySegment a, MemorySegment x, float beta, MemorySegment y) {
        validatePositive("rows", rows);
        validatePositive("cols", cols);
        validateFloatSegment("a", a, (long) rows * cols);
        // x length: cols when A is not transposed (computing A*x of shape rows x cols, x of cols),
        // else rows (computing A^T*x where A^T is cols x rows, x of rows).
        long xLen = trans ? rows : cols;
        long yLen = trans ? cols : rows;
        validateFloatSegment("x", x, xLen);
        validateFloatSegment("y", y, yLen);
        lib.sgemv(ROW_MAJOR, trans ? TRANS : NO_TRANS, rows, cols, alpha, a, cols, x, 1, beta, y, 1);
    }

    /**
     * Single-precision general matrix-vector multiply with {@code float[]} inputs. Elements are
     * wrapped as heap {@link MemorySegment}s and passed via the critical downcall without copying.
     *
     * @see #sgemv(boolean, int, int, float, MemorySegment, MemorySegment, float, MemorySegment)
     */
    public void sgemv(boolean trans, int rows, int cols, float alpha, float[] a, float[] x, float beta, float[] y) {
        sgemv(trans, rows, cols, alpha, MemorySegment.ofArray(a), MemorySegment.ofArray(x), beta, MemorySegment.ofArray(y));
    }

    private static void validatePositive(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got " + value);
        }
    }

    private static void validateFloatSegment(String name, MemorySegment seg, long minElements) {
        long required = minElements * JAVA_FLOAT.byteSize();
        if (seg.byteSize() < required) {
            throw new IllegalArgumentException(
                "Segment '"
                    + name
                    + "' must hold at least "
                    + minElements
                    + " float(s) ("
                    + required
                    + " bytes), but has only "
                    + seg.byteSize()
                    + " bytes"
            );
        }
    }
}
