/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
    public int compress(ByteBuffer dst, ByteBuffer src, int level) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        assert dst.isDirect();
        assert dst.isReadOnly() == false;
        assert src.isDirect();
        assert src.isReadOnly() == false;
        long ret = zstdLib.compress(dst, src, level);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * Compress the content of {@code src} into {@code dst}, and return the number of decompressed bytes. {@link ByteBuffer#position()} and
     * {@link ByteBuffer#limit()} of both {@link ByteBuffer}s are left unmodified.
     */
    public int decompress(ByteBuffer dst, ByteBuffer src) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        assert dst.isDirect();
        assert dst.isReadOnly() == false;
        assert src.isDirect();
        assert src.isReadOnly() == false;
        long ret = zstdLib.decompress(dst, src);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
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
