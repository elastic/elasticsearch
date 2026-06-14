/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.foreign.Critical;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.LibrarySpecification;

import java.lang.foreign.MemorySegment;

@LibrarySpecification(name = "zstd")
public interface ZstdLibrary {

    @Function("ZSTD_compressBound")
    long compressBound(long srcSize);

    @Function("ZSTD_compress")
    long compress(MemorySegment dst, long dstCap, MemorySegment src, long srcSize, int level);

    @Function("ZSTD_compress")
    @Critical
    long compressHeap(MemorySegment dst, long dstCap, MemorySegment src, long srcSize, int level);

    @Function("ZSTD_decompress")
    long decompress(MemorySegment dst, long dstCap, MemorySegment src, long srcSize);

    @Function("ZSTD_decompress")
    @Critical
    long decompressHeap(MemorySegment dst, long dstCap, MemorySegment src, long srcSize);

    @Function("ZSTD_isError")
    boolean isError(long code);

    @Function("ZSTD_getErrorName")
    String getErrorName(long code);

    @Function("ZSTD_createDStream")
    MemorySegment createDStream();

    @Function("ZSTD_freeDStream")
    long freeDStream(MemorySegment dstream);

    @Function("ZSTD_DStreamInSize")
    long dStreamInSize();

    @Function("ZSTD_DStreamOutSize")
    long dStreamOutSize();

    @Function("ZSTD_decompressStream")
    long decompressStream(MemorySegment dstream, MemorySegment output, MemorySegment input);
}
