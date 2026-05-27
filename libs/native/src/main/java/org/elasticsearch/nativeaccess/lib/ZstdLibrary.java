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

    /**
     * One-shot heap {@code byte[]} decompress variant — the Panama equivalent of the
     * {@code com.github.luben.zstd.Zstd.decompress(byte[], byte[])} one-shot API. Bound with
     * {@code LinkerHelperUtil.critical()} since this is a flat downcall (no embedded struct), so
     * the heap segments can be passed directly without an off-heap staging copy. Returns the
     * libzstd byte count (or an error code observable via {@link #isError(long)}).
     */
    long decompress(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize);

    /**
     * One-shot heap {@code byte[]} compress variant at the given compression {@code level}. The
     * caller is responsible for sizing {@code dst} to at least {@link #compressBound(int)} bytes
     * for {@code srcSize}; the returned value is the actual compressed length, or an error code
     * observable via {@link #isError(long)}. Bound with {@code LinkerHelperUtil.critical()}.
     */
    long compress(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize, int level);

    /**
     * Create a streaming decompression context ({@code ZSTD_DStream*}) for incremental decompression
     * of one or more concatenated zstd frames. The returned handle must be closed to release native
     * resources; it is single-threaded by contract.
     */
    DStream createDStream();

    /**
     * Recommended size of the input buffer to feed {@link DStream#decompress} ({@code ZSTD_DStreamInSize}).
     * Constant across the lifetime of libzstd; cache the value on first call.
     */
    long dStreamInSize();

    /**
     * Recommended size of the output buffer for {@link DStream#decompress} ({@code ZSTD_DStreamOutSize}).
     * Only used by {@code skip()} in the wrapper; the regular read path writes straight into the caller's array.
     */
    long dStreamOutSize();

    /**
     * Streaming decompression context. Wraps a {@code ZSTD_DStream*} plus the two persistent
     * {@code ZSTD_inBuffer}/{@code ZSTD_outBuffer} struct holders the binding reuses across calls.
     */
    interface DStream extends AutoCloseable {

        /**
         * Calls {@code ZSTD_decompressStream} feeding {@code src[srcPos..srcLen)} into the context
         * and writing decompressed bytes into {@code dst[dstPos..dstLen)}. Returns the libzstd hint:
         * 0 means the current frame finished decoding, a positive value is an indicative byte count
         * of further input the decoder would like to see, and an error code can be detected with
         * {@link ZstdLibrary#isError(long)} on the caller side (the SPI does not raise).
         */
        long decompress(byte[] dst, int dstPos, int dstLen, byte[] src, int srcPos, int srcLen);

        /** Advanced output position after the most recent {@link #decompress} call (absolute, not delta). */
        int lastDstPos();

        /** Advanced input position after the most recent {@link #decompress} call (absolute, not delta). */
        int lastSrcPos();

        /** Idempotent {@code ZSTD_freeDStream}. */
        @Override
        void close();
    }
}
