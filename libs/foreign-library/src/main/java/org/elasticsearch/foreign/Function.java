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
 * Specifies the exact C symbol that backs a method on a {@link LibrarySpecification @LibrarySpecification}
 * interface. The symbol name is never inferred from the Java method name, so multiple Java methods
 * may bind to the same C function with different argument types or linker options.
 *
 * <p>For example, two Java methods bind to {@code ZSTD_compress}: a baseline binding and a heap-friendly
 * variant marked {@link Critical @Critical}:
 *
 * <pre>{@code
 * @Function("ZSTD_compress")
 * long compress(MemorySegment dst, long dstCap, MemorySegment src, long srcSize, int level);
 *
 * @Function("ZSTD_compress")
 * @Critical
 * long compressHeap(MemorySegment dst, long dstCap, MemorySegment src, long srcSize, int level);
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface Function {
    /** The exact C symbol name to bind. */
    String value();
}
