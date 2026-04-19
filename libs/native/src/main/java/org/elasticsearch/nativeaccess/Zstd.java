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

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.Objects;

public final class Zstd {

    private static final MethodHandle DECOMPRESS_HANDLE;
    static {
        try {
            var lookup = MethodHandles.lookup();
            var mt = MethodType.methodType(int.class, MemorySegment.class, int.class, MemorySegment.class, int.class);
            DECOMPRESS_HANDLE = lookup.findVirtual(Zstd.class, "decompressSegments", mt);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

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
     * Returns a {@link MethodHandle} for decompression that accepts MemorySegment parameters. The handle
     * type is {@code (Zstd, MemorySegment dst, int dstSize, MemorySegment src, int srcSize) -> int}.
     * Segments may be native (e.g. mmap) or heap-backed (on JDK 22+, via the critical linker option).
     * This avoids exposing MemorySegment in the public type signature while enabling callers with
     * {@code java.lang.foreign} access to invoke zero-copy decompression directly.
     */
    public static MethodHandle decompressHandle() {
        return DECOMPRESS_HANDLE;
    }

    @SuppressWarnings("unused") // invoked via DECOMPRESS_HANDLE
    private int decompressSegments(MemorySegment dst, int dstSize, MemorySegment src, int srcSize) {
        assert checkSegmentArgs(dst, dstSize, src, srcSize);
        long ret = zstdLib.decompress(dst, dstSize, src, srcSize);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    static boolean checkSegmentArgs(MemorySegment dst, int dstSize, MemorySegment src, int srcSize) {
        if (dst == null) throw new AssertionError("Null destination segment");
        if (src == null) throw new AssertionError("Null source segment");
        if (dstSize < 0 || dstSize > dst.byteSize()) {
            throw new AssertionError("dstSize=" + dstSize + ", segment size=" + dst.byteSize());
        }
        if (srcSize < 0 || srcSize > src.byteSize()) {
            throw new AssertionError("srcSize=" + srcSize + ", segment size=" + src.byteSize());
        }
        return true;
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
