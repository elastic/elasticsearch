/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.zstd;

import com.sun.jna.Library;
import com.sun.jna.Native;

import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

/** JNA bindings for ZSTD. */
public final class Zstd {

    private static class Holder {
        private static final ZstdLibrary LIBRARY;

        static {
            LIBRARY = AccessController.doPrivileged((PrivilegedAction<ZstdLibrary>) () -> Native.load("zstd", ZstdLibrary.class));
        }

    }

    private interface ZstdLibrary extends Library {

        long ZSTD_compressBound(int scrLen);

        long ZSTD_compress(ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen, int compressionLevel);

        boolean ZSTD_isError(long code);

        String ZSTD_getErrorName(long code);

        long ZSTD_decompress(ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen);

    }

    private static ZstdLibrary library() {
        return Holder.LIBRARY;
    }

    private Zstd() {} // no instantiation

    /**
     * Compress the content of {@code src} into {@code dst} at compression level {@code level}, and return the number of compressed bytes.
     * {@link ByteBuffer#position()} and {@link ByteBuffer#limit()} of both {@link ByteBuffer}s are left unmodified.
     */
    public static int compress(ByteBuffer dst, ByteBuffer src, int level) {
        long ret = library().ZSTD_compress(dst, dst.remaining(), src, src.remaining(), level);
        if (library().ZSTD_isError(ret)) {
            throw new IllegalArgumentException(library().ZSTD_getErrorName(ret));
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
        long ret = library().ZSTD_decompress(dst, dst.remaining(), src, src.remaining());
        if (library().ZSTD_isError(ret)) {
            throw new IllegalArgumentException(library().ZSTD_getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * Return the maximum number of compressed bytes given an input length.
     */
    public static int getMaxCompressedLen(int srcLen) {
        long ret = library().ZSTD_compressBound(srcLen);
        if (library().ZSTD_isError(ret)) {
            throw new IllegalArgumentException(library().ZSTD_getErrorName(ret));
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
