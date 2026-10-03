/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.blas;

import org.elasticsearch.foreign.Critical;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.Platform;

import java.lang.foreign.MemorySegment;

/**
 * FFM binding for the single-precision CBLAS routines in
 * <a href="https://github.com/OpenMathLib/OpenBLAS">OpenBLAS</a>.
 *
 * <p>CBLAS order and transpose arguments are C {@code int} constants:
 * <ul>
 *   <li>{@code CblasRowMajor = 101} — row-major storage</li>
 *   <li>{@code CblasNoTrans = 111} — matrix is not transposed</li>
 *   <li>{@code CblasTrans = 112} — matrix is transposed</li>
 * </ul>
 *
 * <p>The {@code blasint} type used by OpenBLAS is 32-bit ({@code int}) because the library
 * is built without {@code INTERFACE64}. {@code lda}/{@code ldb}/{@code ldc} are the leading
 * dimensions (row strides) in units of elements.
 *
 * <p>Both bindings use {@link Critical @Critical} with {@link Critical.UnsupportedFallback},
 * so they are only available on JDK 22+. On JDK 22+ the critical linker option allows
 * on-heap {@link MemorySegment} arguments (from {@link MemorySegment#ofArray}) to be passed
 * without a copy, at the cost of stalling safepoints for the duration of the call.
 *
 * <p>OpenBLAS is built single-threaded ({@code USE_THREAD=0}); callers that want parallelism
 * distribute independent calls across ES executor threads.
 */
@LibrarySpecification(name = "openblas", unavailableOn = { Platform.DARWIN_X64, Platform.DARWIN_AARCH64 })
interface BlasLibrary {

    /**
     * Single-precision general matrix multiply: {@code C = alpha*op(A)*op(B) + beta*C}.
     *
     * <p>Binds {@code cblas_sgemm} from the OpenBLAS CBLAS interface.
     *
     * @param order  storage order — use {@code 101} (CblasRowMajor)
     * @param transA transpose flag for A — {@code 111} (CblasNoTrans) or {@code 112} (CblasTrans)
     * @param transB transpose flag for B — {@code 111} (CblasNoTrans) or {@code 112} (CblasTrans)
     * @param m      rows of op(A) and C
     * @param n      columns of op(B) and C
     * @param k      columns of op(A) / rows of op(B)
     * @param alpha  scalar multiplier for A*B
     * @param a      segment holding the A matrix (at least {@code m*k} floats when not transposed)
     * @param lda    leading dimension of A (stride between rows, in elements)
     * @param b      segment holding the B matrix (at least {@code k*n} floats when not transposed)
     * @param ldb    leading dimension of B (stride between rows, in elements)
     * @param beta   scalar multiplier for the initial C content
     * @param c      segment holding the C matrix (at least {@code m*n} floats); written in-place
     * @param ldc    leading dimension of C (stride between rows, in elements)
     */
    @Function("cblas_sgemm")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    void sgemm(
        int order,
        int transA,
        int transB,
        int m,
        int n,
        int k,
        float alpha,
        MemorySegment a,
        int lda,
        MemorySegment b,
        int ldb,
        float beta,
        MemorySegment c,
        int ldc
    );

    /**
     * Single-precision general matrix-vector multiply: {@code y = alpha*op(A)*x + beta*y}.
     *
     * <p>Binds {@code cblas_sgemv} from the OpenBLAS CBLAS interface.
     *
     * @param order  storage order — use {@code 101} (CblasRowMajor)
     * @param trans  transpose flag for A — {@code 111} (CblasNoTrans) or {@code 112} (CblasTrans)
     * @param m      rows of A
     * @param n      columns of A
     * @param alpha  scalar multiplier for A*x
     * @param a      segment holding the A matrix (at least {@code m*n} floats)
     * @param lda    leading dimension of A (stride between rows, in elements)
     * @param x      input vector segment (at least {@code n} floats when not transposed, else {@code m})
     * @param incX   stride between successive elements of x (usually 1)
     * @param beta   scalar multiplier for the initial y content
     * @param y      output vector segment (at least {@code m} floats when not transposed, else {@code n})
     * @param incY   stride between successive elements of y (usually 1)
     */
    @Function("cblas_sgemv")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    void sgemv(
        int order,
        int trans,
        int m,
        int n,
        float alpha,
        MemorySegment a,
        int lda,
        MemorySegment x,
        int incX,
        float beta,
        MemorySegment y,
        int incY
    );
}
