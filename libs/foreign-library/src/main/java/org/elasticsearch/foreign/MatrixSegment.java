/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.foreign.MemorySegment;

/**
 * An annotation applicable to {@link MemorySegment} parameters on {@code @Function} methods, where the segment
 * holds a "2D" native array. The annotation describes the shape/size of the 2D array.
 * The processor uses this annotation to emit a fixed bounds check at the top of the
 * generated {@code $Impl} method, before the native call.
 *
 * <p>The row size is given in exactly one of two forms:
 * <ul>
 *   <li>{@link #colsParam()} + exactly one of {@link #elementBytes()}/{@link #elementBits()} — row size
 *   is computed as {@code cols * elementBits / 8}.</li>
 *   <li>{@link #rowBytesParam()} — direct row size in bytes. Mutually exclusive with
 *   {@code colsParam}/{@code elementBytes}/{@code elementBits}.</li>
 * </ul>
 *
 * <p>By default, rows are assumed packed contiguously (row N+1 starts immediately after row N). If
 * instead the buffer pads each row to a fixed stride, set {@link #rowPitchBytesParam()} to the sibling
 * parameter holding that per-row stride in bytes. When set, the required segment size becomes
 * {@code rows * rowPitchBytes} instead of {@code rows * rowBytes}, and the processor additionally
 * emits a check for {@code (rowPitchBytes < rowBytes)}.
 *
 * <pre>{@code
 * @Function("dot_product_i7u_bulk")
 * void dotProductI7uBulk(
 *     @MatrixSegment(rowsParam = "count", colsParam = "length", elementBytes = 1) MemorySegment a,
 *     @VectorSegment(countParam = "length", elementBytes = 1) MemorySegment b,
 *     int length, int count,
 *     @VectorSegment(countParam = "count", elementBytes = 4) MemorySegment result);
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
@BoundsCheck
public @interface MatrixSegment {

    /** Name of the sibling {@code int}/{@code long} parameter holding the row count. */
    String rowsParam();

    /** Name of the sibling {@code int}/{@code long} parameter holding the column count. */
    String colsParam() default "";

    /** Whole-byte element size, used with {@link #colsParam()}. Mutually exclusive with {@link #elementBits()}. */
    int elementBytes() default 0;

    /**
     * Sub-byte element size in bits, used with {@link #colsParam()}. Mutually exclusive with
     * {@link #elementBytes()}.
     */
    int elementBits() default 0;

    /**
     * Name of a sibling {@code int}/{@code long} parameter that holds directly a row size in bytes.
     * Mutually exclusive with {@link #colsParam()}, {@link #elementBytes()}, and {@link #elementBits()}.
     */
    String rowBytesParam() default "";

    /**
     * Name of a sibling {@code int}/{@code long} parameter holding the actual per-row stride in bytes,
     * for buffers where rows are padded rather than packed back-to-back. Optional; compatible with
     * either {@link #colsParam()} or {@link #rowBytesParam()}. When set, the required size becomes
     * {@code rows * rowPitchBytes} instead of {@code rows * rowBytes}.
     */
    String rowPitchBytesParam() default "";

    /**
     * Whether the segment's address must be aligned to {@link #elementBytes()}. Emitted as a JVM
     * {@code assert}, so it only runs under {@code -ea} — zero production cost, matching today's
     * debug-only alignment checks. Requires the {@link #colsParam()} + {@link #elementBytes()} form
     * ({@link #rowBytesParam()} has no declared element scalar to align to, and sub-byte
     * {@link #elementBits()} has no natural alignment unit).
     */
    boolean aligned() default false;
}
