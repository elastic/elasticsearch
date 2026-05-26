/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class Zstd {

    private final ZstdLibrary zstdLib;

    Zstd(ZstdLibrary zstdLib) {
        this.zstdLib = zstdLib;
    }

    /**
     * Compress the content of {@code src} into {@code dst} at compression level {@code level}, and return the number of compressed bytes.
     * {@link ByteBuffer#position()} and {@link ByteBuffer#limit()} of both {@link ByteBuffer}s are left unmodified.
     */
    public int compress(CloseableByteBuffer dst, CloseableByteBuffer src, int level) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        long ret = zstdLib.compress(dst, src, level);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * Decompress the content of {@code src} into {@code dst}, and return the number of decompressed bytes. {@link ByteBuffer#position()}
     * and {@link ByteBuffer#limit()} of both {@link ByteBuffer}s are left unmodified.
     */
    public int decompress(CloseableByteBuffer dst, CloseableByteBuffer src) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        long ret = zstdLib.decompress(dst, src);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * Variant of {@link #decompress(CloseableByteBuffer, CloseableByteBuffer)} that accepts a direct {@link ByteBuffer} as the source.
     * Use this when the caller already holds a direct buffer (e.g. from {@code DirectAccessInput.withByteBufferSlice}) to avoid allocating
     * an intermediate {@link CloseableByteBuffer}.
     */
    public int decompress(CloseableByteBuffer dst, ByteBuffer src) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        if (src.isDirect() == false) {
            throw new IllegalArgumentException("Source buffer must be direct");
        }
        long ret = zstdLib.decompress(dst, src);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * Decompress {@code srcSize} bytes starting at {@code srcOffset} of the direct {@link ByteBuffer} {@code src} into
     * {@code dstSize} bytes starting at {@code dstOffset} of the direct {@link ByteBuffer} {@code dst}, and return the
     * number of decompressed bytes. Both buffers must be direct. {@link ByteBuffer#position()} and {@link ByteBuffer#limit()}
     * of both buffers are left unmodified — callers manage their own cursors. Suits external codec APIs that pass plain
     * {@link ByteBuffer}s with explicit offsets/sizes (e.g. parquet-mr's {@code BytesInputDecompressor}).
     */
    public int decompress(ByteBuffer dst, int dstOffset, int dstSize, ByteBuffer src, int srcOffset, int srcSize) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        if (dst.isDirect() == false) {
            throw new IllegalArgumentException("Destination buffer must be direct");
        }
        if (src.isDirect() == false) {
            throw new IllegalArgumentException("Source buffer must be direct");
        }
        checkRange("Destination", dstOffset, dstSize, dst.capacity());
        checkRange("Source", srcOffset, srcSize, src.capacity());
        long ret = zstdLib.decompress(dst, dstOffset, dstSize, src, srcOffset, srcSize);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    private static void checkRange(String label, int offset, int size, int capacity) {
        // The (offset > capacity - size) form avoids the (offset + size > capacity) overflow
        // that could otherwise wrap around when offset + size exceeds Integer.MAX_VALUE.
        if (offset < 0 || size < 0 || offset > capacity - size) {
            throw new IllegalArgumentException(
                label + " range [offset=" + offset + ", size=" + size + ") is out of bounds for buffer with capacity " + capacity
            );
        }
    }

    /**
     * Return the maximum number of compressed bytes given an input length.
     */
    public int compressBound(int srcLen) {
        long ret = zstdLib.compressBound(srcLen);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                srcLen
                    + " bytes may require up to "
                    + Long.toUnsignedString(ret)
                    + " bytes, which overflows the maximum capacity of a ByteBuffer"
            );
        }
        return (int) ret;
    }
}
