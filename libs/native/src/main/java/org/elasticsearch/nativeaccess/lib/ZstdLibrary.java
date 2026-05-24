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
     * Decompress variant that accepts direct {@link ByteBuffer}s on both sides with explicit
     * offsets and lengths. Suits callers driven by an external codec API (e.g. parquet-mr's
     * {@code BytesInputDecompressor}) where compressed and decompressed sizes are known
     * up front and the buffers are managed outside this library. Both buffers must be direct.
     * Neither buffer's position or limit is modified.
     */
    long decompress(ByteBuffer dst, int dstOffset, int dstSize, ByteBuffer src, int srcOffset, int srcSize);
}
