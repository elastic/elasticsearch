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
import com.sun.jna.Pointer;

import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

class JnaZstdLibrary implements ZstdLibrary {

    private interface NativeFunctions extends Library {
        long ZSTD_compressBound(int scrLen);

        long ZSTD_compress(Pointer dst, int dstLen, Pointer src, int srcLen, int compressionLevel);

        boolean ZSTD_isError(long code);

        String ZSTD_getErrorName(long code);

        long ZSTD_decompress(Pointer dst, int dstLen, Pointer src, int srcLen);
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
    public long compress(CloseableByteBuffer dst, CloseableByteBuffer src, int compressionLevel) {
        assert dst instanceof JnaCloseableByteBuffer;
        assert src instanceof JnaCloseableByteBuffer;
        var nativeDst = (JnaCloseableByteBuffer) dst;
        var nativeSrc = (JnaCloseableByteBuffer) src;
        return functions.ZSTD_compress(
            nativeDst.memory.share(dst.buffer().position()),
            dst.buffer().remaining(),
            nativeSrc.memory.share(src.buffer().position()),
            src.buffer().remaining(),
            compressionLevel
        );
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
    public long decompress(CloseableByteBuffer dst, CloseableByteBuffer src) {
        assert dst instanceof JnaCloseableByteBuffer;
        assert src instanceof JnaCloseableByteBuffer;
        var nativeDst = (JnaCloseableByteBuffer) dst;
        var nativeSrc = (JnaCloseableByteBuffer) src;
        return functions.ZSTD_decompress(
            nativeDst.memory.share(dst.buffer().position()),
            dst.buffer().remaining(),
            nativeSrc.memory.share(src.buffer().position()),
            src.buffer().remaining()
        );
    }
}
