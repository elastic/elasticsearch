/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BOOLEAN;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

class JdkZstdLibrary implements ZstdLibrary {

    static {
        System.loadLibrary("zstd");
    }

    private static final MethodHandle compressBound$mh = downcallHandle("ZSTD_compressBound", FunctionDescriptor.of(JAVA_LONG, JAVA_INT));
    private static final MethodHandle compress$mh = downcallHandle(
        "ZSTD_compress",
        FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT)
    );
    private static final MethodHandle isError$mh = downcallHandle("ZSTD_isError", FunctionDescriptor.of(JAVA_BOOLEAN, JAVA_LONG));
    private static final MethodHandle getErrorName$mh = downcallHandle("ZSTD_getErrorName", FunctionDescriptor.of(ADDRESS, JAVA_LONG));
    private static final MethodHandle decompress$mh = downcallHandle(
        "ZSTD_decompress",
        FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT)
    );

    @Override
    public long compressBound(int srcLen) {
        try {
            return (long) compressBound$mh.invokeExact(srcLen);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public long compress(ByteBuffer dst, ByteBuffer src, int compressionLevel) {
        var nativeDst = MemorySegment.ofBuffer(dst);
        var nativeSrc = MemorySegment.ofBuffer(src);
        try {
            return (long) compress$mh.invokeExact(nativeDst, dst.remaining(), nativeSrc, src.remaining(), compressionLevel);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean isError(long code) {
        try {
            return (boolean) isError$mh.invokeExact(code);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public String getErrorName(long code) {
        try {
            MemorySegment str = (MemorySegment) getErrorName$mh.invokeExact(code);
            return str.reinterpret(Long.MAX_VALUE).getUtf8String(0);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public long decompress(ByteBuffer dst, ByteBuffer src) {
        var nativeDst = MemorySegment.ofBuffer(dst);
        var nativeSrc = MemorySegment.ofBuffer(src);
        try {
            return (long) decompress$mh.invokeExact(nativeDst, dst.remaining(), nativeSrc, src.remaining());
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
