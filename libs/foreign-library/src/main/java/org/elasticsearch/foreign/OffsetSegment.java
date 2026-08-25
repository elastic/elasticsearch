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
 * native function accesses a sub-range {@code [offset..offset+length)} of the segment, optionally with
 * additional readable padding bytes past that range.
 *
 * <p>The processor emits
 * {@code Objects.checkFromIndexSize((long) offset, (long) length + paddingBytes, segment.byteSize())}
 * at the top of the generated {@code $Impl} method, before the native call. This verifies that
 * {@code offset + length + paddingBytes <= segment.byteSize()}.
 *
 * <pre>{@code
 * @Function("process_range")
 * int processRange(
 *     @OffsetSegment(offset = "offset", length = "len") MemorySegment buf,
 *     int offset,
 *     int len);
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
@BoundsCheck
public @interface OffsetSegment {

    /** Name of the sibling {@code int}/{@code long} parameter holding the byte offset into the segment. */
    String offset();

    /** Name of the sibling {@code int}/{@code long} parameter holding the byte length of the accessed region. */
    String length();

    /**
     * Constant number of additional readable bytes required past {@code offset + length}.
     * Defaults to 0 (no extra padding required).
     */
    int paddingBytes() default 0;
}
