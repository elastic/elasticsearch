/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal;

import org.elasticsearch.foreign.Critical;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.Platform;
import org.elasticsearch.foreign.SlicedSegment;
import org.elasticsearch.foreign.VectorSegment;

import java.lang.foreign.MemorySegment;

/**
 * FFM binding for the {@code libsimdjson} native library, which wraps
 * <a href="https://github.com/simdjson/simdjson">simdjson</a>'s stage 1
 * structural indexing with auto-selected SIMD backend (AVX-512, AVX2, SSE4.2, NEON).
 *
 * <p>The caller allocates a context once per thread via {@link #create(int)},
 * runs stage 1 on each buffer, reads the resulting structural index array,
 * then destroys the context via {@link #destroy(MemorySegment)}.
 *
 * <p>The {@code @Critical} variants accept heap-backed {@link MemorySegment}s
 * (e.g. {@code MemorySegment.ofArray(byte[])}) without copying — the JVM pins
 * the Java array for the duration of the call. This eliminates the memcpy
 * that the non-critical variants require.
 */
@LibrarySpecification(name = "simdjson", unavailableOn = { Platform.WINDOWS_X64, Platform.DARWIN_X64 })
public interface SimdJsonLibrary {

    /**
     * Allocates a reusable stage 1 context with internal buffers pre-sized for documents up to
     * {@code initialCapacity} bytes. This is a sizing hint, not a hard limit — if {@link #stage1}
     * is later called with a {@code len} exceeding the current capacity, the native code
     * automatically grows its internal buffers to fit.
     *
     * @return an opaque context pointer, or {@link MemorySegment#NULL} on allocation failure
     */
    @Function("simdjson_stage1_create")
    MemorySegment create(int initialCapacity);

    /**
     * Frees a context previously obtained from {@link #create(int)}. Safe to call with
     * {@link MemorySegment#NULL} (the native code treats a null pointer as a no-op).
     * Passing a Java {@code null} reference is not permitted and will throw {@link NullPointerException}.
     */
    @Function("simdjson_stage1_destroy")
    void destroy(MemorySegment ctx);

    /**
     * Runs stage 1 over {@code buf[offset..offset+len)} and writes structural indices into
     * {@code outBuf}. Adds {@code offset} to each index so outputs are absolute positions.
     *
     * <p>Stage 1 copies its remainder block into a stack-local buffer padded with spaces, so
     * the native code never reads past {@code buf[offset + len - 1]}. The {@code @SlicedSegment}
     * bounds check verifies {@code offset + len <= buf.byteSize()}. This is verified by guard-page
     * tests in {@code SimdJsonLibraryTests} and {@code StructuralIndexerTests}.
     *
     * <p>This is the {@code @Critical} variant: {@code buf} and {@code outBuf} may be
     * heap-backed segments (e.g. {@code MemorySegment.ofArray(byte[])}), avoiding off-heap
     * buffer copies entirely. On JDK 21, {@link SimdJsonStage1HeapFallback} stages heap
     * segments instead.
     *
     * @return 0 on success, non-zero simdjson error code on failure, or -2 if output exceeds capacity
     */
    @Function("simdjson_stage1_run")
    @Critical(fallbackAdapter = SimdJsonStage1HeapFallback.class)
    int stage1(
        MemorySegment ctx,
        @SlicedSegment(offsetParam = "offset", sizeParam = "len") MemorySegment buf,
        int offset,
        int len,
        @VectorSegment(countParam = "outBufCapacity", elementBits = 32) MemorySegment outBuf,
        int outBufCapacity,
        MemorySegment outCount
    );

    /**
     * Returns a static, null-terminated error message for an error code returned by {@link #stage1},
     * or {@code null} if the native library returns a null pointer.
     */
    @Function("simdjson_stage1_error_message")
    String errorMessage(int errorCode);
}
