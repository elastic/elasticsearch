/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

/**
 * JDK 21 {@code @Critical} fallback for {@link ZstdLibrary#compressHeap} / {@link ZstdLibrary#decompressHeap}.
 *
 * <p>On JDK 21 the raw libzstd downcall handle is linked without {@code Linker.Option.critical(true)} (the
 * option doesn't exist there), so it rejects heap-backed {@link MemorySegment} arguments. The annotation
 * processor wires {@code @Critical(fallbackAdapter = ZstdHeapFallback.class)} so that on JDK 21 each
 * {@code compressHeap$mh} / {@code decompressHeap$mh} field holds an adapter that points at the matching
 * method below: the heap input is staged into a confined {@link Arena}, the raw handle is invoked with
 * native segments, and the decoded length is copied back into the caller's heap output. On JDK 22+ this class
 * is not referenced.
 */
public final class ZstdHeapFallback {

    private ZstdHeapFallback() {}

    public static long compressHeap(MethodHandle mh, MemorySegment dst, long dstCap, MemorySegment src, long srcSize, int level)
        throws Throwable {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment stagedSrc = stageInput(arena, src, srcSize);
            MemorySegment stagedDst = stageOutput(arena, dst, dstCap);
            long ret = (long) mh.invokeExact(stagedDst, dstCap, stagedSrc, srcSize, level);
            copyBack(stagedDst, dst, dstCap, ret);
            return ret;
        }
    }

    public static long decompressHeap(MethodHandle mh, MemorySegment dst, long dstCap, MemorySegment src, long srcSize) throws Throwable {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment stagedSrc = stageInput(arena, src, srcSize);
            MemorySegment stagedDst = stageOutput(arena, dst, dstCap);
            long ret = (long) mh.invokeExact(stagedDst, dstCap, stagedSrc, srcSize);
            copyBack(stagedDst, dst, dstCap, ret);
            return ret;
        }
    }

    private static MemorySegment stageInput(Arena arena, MemorySegment src, long srcSize) {
        if (src.isNative()) {
            return src;
        }
        MemorySegment staged = arena.allocate(srcSize);
        MemorySegment.copy(src, 0L, staged, 0L, srcSize);
        return staged;
    }

    private static MemorySegment stageOutput(Arena arena, MemorySegment dst, long dstCap) {
        if (dst.isNative()) {
            return dst;
        }
        return arena.allocate(dstCap);
    }

    private static void copyBack(MemorySegment staged, MemorySegment dst, long dstCap, long ret) {
        if (staged == dst) {
            return; // caller passed a native segment; libzstd wrote directly into it
        }
        if (ret >= 0 && ret <= dstCap) {
            MemorySegment.copy(staged, 0L, dst, 0L, ret);
        }
    }
}
