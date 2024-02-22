/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;

import com.sun.jna.Pointer;

import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

import java.nio.ByteBuffer;

public class JnaZstdLibrary implements ZstdLibrary {

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
    public long compress(ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen, int compressionLevel) {
        assert dst.isDirect() == false;
        assert src.isDirect() == false;
        assert dstLen != 0;
        assert srcLen != 0;
        try (Memory nativeDst = new Memory(dstLen);
             Memory nativeSrc = new Memory(srcLen)) {
            nativeSrc.write(0, src.array(), src.position(), srcLen);
            long compressedLen = functions.ZSTD_compress(nativeDst, dstLen, nativeSrc, srcLen, compressionLevel);
            if (functions.ZSTD_isError(compressedLen) == false) {
                assert compressedLen <= dstLen;
                nativeDst.read(0, dst.array(), dst.position(), (int) compressedLen);
            }
            return compressedLen;
        }
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
    public long decompress(ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen) {
        assert dst.isDirect() == false;
        assert src.isDirect() == false;
        try (Memory nativeDst = new Memory(dstLen);
             Memory nativeSrc = new Memory(srcLen)) {
            nativeSrc.write(0, src.array(), src.position(), srcLen);
            long decompressedLen = functions.ZSTD_decompress(nativeDst, dstLen, nativeSrc, srcLen);
            if (functions.ZSTD_isError(decompressedLen) == false) {
                assert decompressedLen <= dstLen;
                nativeDst.read(0, dst.array(), dst.position(), (int) decompressedLen);
            }
            return decompressedLen;
        }
    }
}
