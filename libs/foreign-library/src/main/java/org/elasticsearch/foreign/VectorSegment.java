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
 * holds a linear native array. The annotation describes the shape/size of this array.
 * The processor uses this annotation to emit a fixed bounds check at the top of the
 * generated {@code $Impl} method, before the native call.
 *
 * <p>Exactly one of {@link #elementBytes()} or {@link #elementBits()} must be set; the processor
 * rejects a method where neither, or both, are set. {@link #elementBits()} exists for sub-byte packed
 * element sizes (e.g. {@code 4} for int4); whole-byte element sizes should use
 * {@link #elementBytes()}.
 *
 * <pre>{@code
 * @Function("dot_product_i7u")
 * int dotProductI7u(
 *     @VectorSegment(countParam = "length", elementBytes = 1) MemorySegment a,
 *     @VectorSegment(countParam = "length", elementBytes = 1) MemorySegment b,
 *     int length);
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
@BoundsCheck
public @interface VectorSegment {

    /** Name of the sibling {@code int}/{@code long} parameter holding the element count. */
    String countParam();

    /** Whole-byte element size. Mutually exclusive with {@link #elementBits()}. */
    int elementBytes() default 0;

    /**
     * Sub-byte element size in bits (e.g. {@code 4} for int4). Mutually exclusive with
     * {@link #elementBytes()}.
     */
    int elementBits() default 0;

    /**
     * Whether the segment's address must be aligned to {@link #elementBytes()}. Emitted as a JVM
     * {@code assert}, so it only runs under {@code -ea} — zero production cost, matching today's
     * debug-only alignment checks. Requires {@link #elementBytes()} (sub-byte elements have no
     * natural alignment unit).
     */
    boolean aligned() default false;
}
