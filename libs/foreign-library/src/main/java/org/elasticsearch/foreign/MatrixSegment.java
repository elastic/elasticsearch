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
 * <pre>{@code
 * @Function("dot_product_i7u_bulk")
 * void dotProductI7uBulk(
 *     @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = 8) MemorySegment a,
 *     @VectorSegment(countParam = "length", elementBits = 8) MemorySegment b,
 *     int length, int count,
 *     @VectorSegment(countParam = "count", elementBits = 32) MemorySegment result);
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
@BoundsCheck
public @interface MatrixSegment {

    /** Name of the sibling {@code int}/{@code long} parameter holding the row count. */
    String rowsParam();

    /** Name of the sibling {@code int}/{@code long} parameter holding the column count. */
    String colsParam();

    /** Element size in bits, used with {@link #colsParam()}. */
    int elementBits();

    /**
     * Name of the optional sibling {@code int}/{@code long} parameter holding the row padding
     * bytes. If no parameter is specified, padding is assumed to be 0.
     */
    String paddingBytesParam() default "";

    /**
     * Whether the segment's address must be aligned to the element byte size. Emitted as a JVM
     * {@code assert}, so it only runs under {@code -ea}. Requires {@link #elementBits()} to be
     * a multiple of 8 (whole-bytes).
     */
    boolean aligned() default false;
}
