/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.nativeaccess.CloseableByteBuffer;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

public non-sealed interface ZstdLibrary extends NativeLibrary {

    long compressBound(int scrLen);

    long compress(CloseableByteBuffer dst, CloseableByteBuffer src, int compressionLevel);

    boolean isError(long code);

    String getErrorName(long code);

    long decompress(CloseableByteBuffer dst, CloseableByteBuffer src);

    /**
     * Decompress variant that accepts a direct {@link ByteBuffer} as the source, avoiding the need for a
     * {@link CloseableByteBuffer} wrapper when the caller already holds a direct buffer (e.g. from
     * {@code DirectAccessInput.withByteBufferSlice}).
     */
    long decompress(CloseableByteBuffer dst, ByteBuffer src);

    /**
     * Decompress variant that accepts raw {@link MemorySegment}s for both source and destination.
     * Segments may be native (e.g. mmap) or heap-backed (on JDK 22+, via the critical linker option).
     * This enables fully zero-copy decompression without intermediate buffer conversions.
     */
    long decompress(MemorySegment dst, int dstSize, MemorySegment src, int srcSize);
}
