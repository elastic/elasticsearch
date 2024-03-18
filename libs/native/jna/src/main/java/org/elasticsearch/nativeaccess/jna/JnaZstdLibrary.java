/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;

import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

import java.nio.ByteBuffer;

class JnaZstdLibrary implements ZstdLibrary {

    private interface NativeFunctions extends Library {
        long ZSTD_compressBound(int scrLen);

        long ZSTD_compress(ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen, int compressionLevel);

        boolean ZSTD_isError(long code);

        String ZSTD_getErrorName(long code);

        long ZSTD_decompress(ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen);
    }

    private final NativeFunctions functions;

    JnaZstdLibrary() {
        this.functions = Native.load("zstd", NativeFunctions.class);
    }

    @Override
    public long compressBound(int scrLen) {
        return functions.ZSTD_compressBound(scrLen);
    }

    @Override
    public long compress(ByteBuffer dst, ByteBuffer src, int compressionLevel) {
        return functions.ZSTD_compress(dst, dst.remaining(), src, src.remaining(), compressionLevel);
    }

    @Override
    public boolean isError(long code) {
        return functions.ZSTD_isError(code);
    }

    @Override
    public String getErrorName(long code) {
        return functions.ZSTD_getErrorName(code);
    }

    @Override
    public long decompress(ByteBuffer dst, ByteBuffer src) {
        return functions.ZSTD_decompress(dst, dst.remaining(), src, src.remaining());
    }
}
