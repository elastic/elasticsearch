/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.zstd;

import com.sun.jna.Library;
import com.sun.jna.Native;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/** JNA bindings for ZSTD. */
final class Zstd {

    // TODO: Move this under libs/ and make it public so that we can progressively replace all our usage of DEFLATE with ZSTD?

    private static final ZstdLibrary LIBRARY;

    static {
        File zstdPath;
        try {
            zstdPath = Native.extractFromResourcePath("zstd");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LIBRARY = Native.load(zstdPath.getAbsolutePath(), ZstdLibrary.class);
    }

    private interface ZstdLibrary extends Library {

        long ZSTD_compressBound(int scrLen);

        long ZSTD_compress(ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen, int compressionLevel);

        boolean ZSTD_isError(long code);

        String ZSTD_getErrorName(long code);

        long ZSTD_decompress(ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen);

    }

    /**
     * Compress the content of {@code src} into {@code dst} at compression level {@code level}, and return the number of compressed bytes.
     * {@link ByteBuffer#position()} and {@link ByteBuffer#limit()} of both {@link ByteBuffer}s are left unmodified.
     */
    public static int compress(ByteBuffer dst, ByteBuffer src, int level) {
        long ret = LIBRARY.ZSTD_compress(dst, dst.remaining(), src, src.remaining(), level);
        if (LIBRARY.ZSTD_isError(ret)) {
            throw new IllegalArgumentException(LIBRARY.ZSTD_getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * Compress the content of {@code src} into {@code dst}, and return the number of decompressed bytes. {@link ByteBuffer#position()} and
     * {@link ByteBuffer#limit()} of both {@link ByteBuffer}s are left unmodified.
     */
    public static int decompress(ByteBuffer dst, ByteBuffer src) {
        long ret = LIBRARY.ZSTD_decompress(dst, dst.remaining(), src, src.remaining());
        if (LIBRARY.ZSTD_isError(ret)) {
            throw new IllegalArgumentException(LIBRARY.ZSTD_getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * Return the maximum number of compressed bytes given an input length.
     */
    public static int getMaxCompressedLen(int srcLen) {
        long ret = LIBRARY.ZSTD_compressBound(srcLen);
        if (LIBRARY.ZSTD_isError(ret)) {
            throw new IllegalArgumentException(LIBRARY.ZSTD_getErrorName(ret));
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
