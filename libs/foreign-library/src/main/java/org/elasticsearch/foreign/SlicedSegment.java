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
 * An annotation applicable to {@link MemorySegment} parameters on {@code @Function} methods, where the
 * native function accesses a slice {@code [offset..offset+size)} of the segment.
 *
 * <p>The processor emits
 * {@code Objects.checkFromIndexSize((long) offset, (long) size, segment.byteSize())}
 * at the top of the generated {@code $Impl} method, before the native call. This verifies that
 * {@code offset + size <= segment.byteSize()}.
 *
 * <pre>{@code
 * @Function("process_range")
 * int processRange(
 *     @SlicedSegment(offsetParam = "offset", sizeParam = "size") MemorySegment buf,
 *     int offset,
 *     int size);
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
@BoundsCheck
public @interface SlicedSegment {

    /** Name of the sibling {@code int}/{@code long} parameter holding the byte offset into the segment. */
    String offsetParam();

    /** Name of the sibling {@code int}/{@code long} parameter holding the byte size of the slice. */
    String sizeParam();
}
