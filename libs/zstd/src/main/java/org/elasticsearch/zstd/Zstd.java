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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;

/** JNA bindings for ZSTD. */
public final class Zstd {

    private static ZstdLibrary load() {
        File zstdFile = AccessController.doPrivileged((PrivilegedAction<File>) () -> {
            try {
                return Native.extractFromResourcePath("zstd");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        String zstdPath = zstdFile.getAbsolutePath();
        return AccessController.doPrivileged((PrivilegedAction<ZstdLibrary>) () -> Native.load(zstdPath, ZstdLibrary.class));
    }

    private interface ZstdLibrary extends Library {

        ZstdLibrary INSTANCE = load();

        long ZSTD_compressBound(int scrLen);

        long ZSTD_compress(ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen, int compressionLevel);

        boolean ZSTD_isError(long code);

        String ZSTD_getErrorName(long code);

        long ZSTD_decompress(ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen);

    }

    private static ZstdLibrary library() {
        return ZstdLibrary.INSTANCE;
    }

    private Zstd() {} // no instantiation

    // NOTE: When calling compress/decompress functions, the ZSTD documentation advises to reuse ZSTD_CCtx/ZSTD_DCtx objects across calls
    // for efficiency. Functions that allow reusing an existing context struct are not exposed here, only their counterparts that allocate
    // these context structs as part of the compress/decompress function call. We may consider exposing these functions if we were to
    // compress/decompress multiple inputs in sequence.

    /**
     * Compress the content of {@code src} into {@code dst} at compression level {@code level}, and return the number of compressed bytes.
     * {@link ByteBuffer#position()} and {@link ByteBuffer#limit()} of both {@link ByteBuffer}s are left unmodified.
     */
    public static int compress(ByteBuffer dst, ByteBuffer src, int level) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
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
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
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
    public static int compressBound(int srcLen) {
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
