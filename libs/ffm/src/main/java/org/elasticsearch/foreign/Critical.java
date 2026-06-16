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

/**
 * Marks a {@link Function @Function} binding as <em>critical</em>, asking the FFM linker to use the
 * critical calling convention. Critical calls avoid the safepoint transition that wraps normal native
 * calls, which is faster for short, leaf-style functions but forbids upcalls back into Java and
 * pins any on-heap {@link java.lang.foreign.MemorySegment} arguments for the duration of the call.
 *
 * <p>Use {@code @Critical} for hot, well-bounded native routines such as compression primitives.
 * Pair it with {@link Function @Function} on the same method:
 *
 * <pre>{@code
 * @Function("ZSTD_decompress")
 * @Critical
 * long decompressHeap(MemorySegment dst, long dstCap, MemorySegment src, long srcSize);
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface Critical {
}
