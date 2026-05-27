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

    /**
     * One-shot heap {@code byte[]} decompress for callers that already hold the full compressed
     * frame in a Java array (e.g. parquet-mr's {@code BytesInputDecompressor#decompress(BytesInput,int)}).
     * Equivalent to {@code com.github.luben.zstd.Zstd.decompress(byte[], byte[])} but routed through
     * Panama FFI's {@code critical(true)} downcall — the heap segments are passed through directly
     * without an off-heap staging copy. Returns the number of decompressed bytes actually written
     * into {@code dst[dstOffset .. dstOffset+dstSize)}.
     */
    public int decompress(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        checkRange("Destination", dstOffset, dstSize, dst.length);
        checkRange("Source", srcOffset, srcSize, src.length);
        long ret = zstdLib.decompress(dst, dstOffset, dstSize, src, srcOffset, srcSize);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * One-shot heap {@code byte[]} compress at the given zstd compression {@code level}. The caller
     * must size {@code dst} to at least {@link #compressBound(int)} bytes for {@code srcSize};
     * Parquet's {@code BytesInputCompressor} sizes its output buffer based on libzstd's worst case.
     * Returns the actual compressed length written into {@code dst[dstOffset .. dstOffset+returned)}.
     */
    public int compress(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize, int level) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        checkRange("Destination", dstOffset, dstSize, dst.length);
        checkRange("Source", srcOffset, srcSize, src.length);
        long ret = zstdLib.compress(dst, dstOffset, dstSize, src, srcOffset, srcSize, level);
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

    /**
     * Recommended size for the input buffer feeding {@link DStream#decompress} ({@code ZSTD_DStreamInSize}).
     * The value is constant per libzstd build (≈ 128 KB on 1.5.7); callers typically cache it in a
     * {@code static final} field rather than calling per-stream.
     */
    public int dStreamInSize() {
        return checkSize(zstdLib.dStreamInSize(), "dStreamInSize");
    }

    /**
     * Recommended size for the output buffer of {@link DStream#decompress} ({@code ZSTD_DStreamOutSize}).
     * Used only by the {@code skip()} scratch buffer in the wrapper — the regular read path writes
     * straight into the caller's destination array.
     */
    public int dStreamOutSize() {
        return checkSize(zstdLib.dStreamOutSize(), "dStreamOutSize");
    }

    private int checkSize(long ret, String which) {
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret <= 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Unexpected " + which + " value: " + Long.toUnsignedString(ret));
        }
        return (int) ret;
    }

    /**
     * Allocate a streaming decompression context wrapping {@code ZSTD_DStream}. The returned
     * {@link DStream} owns a native zstd context plus two small persistent struct holders;
     * call {@link DStream#close()} (or use try-with-resources) to release them. Single-threaded
     * by contract — do not share a single {@link DStream} across threads.
     */
    public DStream newDStream() {
        return new DStream(zstdLib.createDStream());
    }

    /**
     * Streaming-mode zstd decompression resource. Backs {@code PanamaZstdInputStream} and any
     * other consumer that needs incremental {@code ZSTD_decompressStream} semantics: refill an
     * input chunk, call {@link #decompress}, read {@link #lastSrcPos()} / {@link #lastDstPos()} to
     * advance cursors, loop while the hint return is positive (more input wanted), and observe a
     * zero return when the current frame finished.
     *
     * <p>Bounds are validated Java-side; error returns from libzstd are translated to
     * {@link IllegalArgumentException} carrying the {@code ZSTD_getErrorName} string. All hot-path
     * state (the two {@code ZSTD_inBuffer}/{@code ZSTD_outBuffer} struct holders, the native context
     * handle) lives inside the binding — every {@link #decompress} call is allocation-free.
     *
     * <p>Not thread-safe.
     */
    public final class DStream implements AutoCloseable {

        private final ZstdLibrary.DStream impl;
        private boolean closed;

        DStream(ZstdLibrary.DStream impl) {
            this.impl = impl;
        }

        /**
         * Feed {@code src[srcPos..srcLen)} into the decoder and write decompressed bytes into
         * {@code dst[dstPos..dstLen)}. Returns the libzstd hint: {@code 0} means the current frame
         * finished decoding (the decoder is implicitly reset for the next concatenated frame, if any),
         * a positive return is libzstd's best guess for the amount of additional input it would like
         * to see next, and a zstd-side error is translated to {@link IllegalArgumentException}.
         *
         * <p>After return, the caller reads {@link #lastSrcPos()} / {@link #lastDstPos()} to learn
         * how far the input/output cursors advanced — these are absolute offsets within the supplied
         * arrays, not deltas.
         *
         * <p><b>Chunking contract.</b> Callers must pass at most {@link Zstd#dStreamInSize()} bytes
         * of input per call ({@code srcLen - srcPos <= dStreamInSize()}). Larger slices raise
         * {@link IllegalArgumentException} from the binding — the binding hard-fails rather than
         * silently consuming a prefix because partial consumption is easy to overlook at call sites.
         * {@code PanamaZstdInputStream} caches the recommended size in a {@code static final} and
         * refills upstream in chunks of that size. Output may be any size; the binding internally
         * caps each call at {@link Zstd#dStreamOutSize()} and the caller's outer loop drives
         * subsequent calls as needed.
         */
        public long decompress(byte[] dst, int dstPos, int dstLen, byte[] src, int srcPos, int srcLen) {
            if (closed) {
                throw new IllegalStateException("DStream is closed");
            }
            Objects.requireNonNull(dst, "Null destination buffer");
            Objects.requireNonNull(src, "Null source buffer");
            Objects.checkFromToIndex(dstPos, dstLen, dst.length);
            Objects.checkFromToIndex(srcPos, srcLen, src.length);
            long ret = impl.decompress(dst, dstPos, dstLen, src, srcPos, srcLen);
            if (zstdLib.isError(ret)) {
                throw new IllegalArgumentException(zstdLib.getErrorName(ret));
            }
            return ret;
        }

        /**
         * Output position after the most recent {@link #decompress} — an absolute index into the
         * {@code dst} array that was passed to that call.
         */
        public int lastDstPos() {
            return impl.lastDstPos();
        }

        /**
         * Input position after the most recent {@link #decompress} — an absolute index into the
         * {@code src} array that was passed to that call.
         */
        public int lastSrcPos() {
            return impl.lastSrcPos();
        }

        /** Idempotent — frees the native {@code ZSTD_DStream} and the cached struct holders. */
        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                impl.close();
            }
        }
    }
}
