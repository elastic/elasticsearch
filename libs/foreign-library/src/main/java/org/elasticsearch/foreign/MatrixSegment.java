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
 *   <li>{@link #cols()} + exactly one of {@link #elementBytes()}/{@link #elementBits()} — row size is
 *   computed as {@code cols * elementBits / 8}.</li>
 *   <li>{@link #rowBytes()} — direct row size in bytes. Mutually exclusive with {@code cols}/{@code elementBytes}/
 *   {@code elementBits}.</li>
 * </ul>
 *
 * <pre>{@code
 * @Function("dot_product_i7u_bulk")
 * void dotProductI7uBulk(
 *     @MatrixSegment(rows = "count", cols = "length", elementBytes = 1) MemorySegment a,
 *     @VectorSegment(count = "length", elementBytes = 1) MemorySegment b,
 *     int length, int count,
 *     @VectorSegment(count = "count", elementBytes = 4) MemorySegment result);
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
@BoundsCheck
public @interface MatrixSegment {

    /** Name of the sibling {@code int}/{@code long} parameter holding the row count. */
    String rows();

    /** Name of the sibling {@code int}/{@code long} parameter holding the column count. */
    String cols() default "";

    /** Whole-byte element size, used with {@link #cols()}. Mutually exclusive with {@link #elementBits()}. */
    int elementBytes() default 0;

    /**
     * Sub-byte element size in bits, used with {@link #cols()}. Mutually exclusive with
     * {@link #elementBytes()}.
     */
    int elementBits() default 0;

    /**
     * Name of a sibling {@code int}/{@code long} parameter that holds directly a row size in bytes.
     * Mutually exclusive with {@link #cols()}, {@link #elementBytes()}, and {@link #elementBits()}.
     */
    String rowBytes() default "";
}
