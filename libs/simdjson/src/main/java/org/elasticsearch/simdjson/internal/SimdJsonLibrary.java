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
import org.elasticsearch.foreign.OffsetSegment;
import org.elasticsearch.foreign.Platform;
import org.elasticsearch.foreign.VectorSegment;

import java.lang.foreign.MemorySegment;

/**
 * FFM binding for the {@code libes_simdjson} native library, which wraps
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
@LibrarySpecification(name = "es_simdjson", unavailableOn = { Platform.WINDOWS_X64, Platform.DARWIN_X64 })
public interface SimdJsonLibrary {

    /**
     * Allocates a reusable stage 1 context with internal buffers pre-sized for documents up to
     * {@code initialCapacity} bytes. This is a sizing hint, not a hard limit — if {@link #stage1}
     * is later called with a {@code len} exceeding the current capacity, the native code
     * automatically grows its internal buffers to fit.
     *
     * @return an opaque context pointer, or {@link MemorySegment#NULL} on allocation failure
     */
    @Function("es_stage1_create")
    MemorySegment create(int initialCapacity);

    /**
     * Frees a context previously obtained from {@link #create(int)}. Safe to call with
     * {@link MemorySegment#NULL} (the native code treats a null pointer as a no-op).
     * Passing a Java {@code null} reference is not permitted and will throw {@link NullPointerException}.
     */
    @Function("es_stage1_destroy")
    void destroy(MemorySegment ctx);

    /**
     * Runs stage 1 over {@code buf[offset..offset+len)} and writes structural indices into
     * {@code outBuf}. Adds {@code offset} to each index so outputs are absolute positions.
     *
     * <p>Stage 1 copies its remainder block into a stack-local buffer padded with spaces, so
     * the native code never reads past {@code buf[offset + len - 1]}. The {@code paddingBytes = 0}
     * on the {@code @OffsetSegment} reflects this — the bounds check verifies
     * {@code offset + len <= buf.byteSize()} with no extra slack. This is verified by guard-page
     * tests in {@code SimdJsonLibraryTests} and {@code StructuralIndexerTests}.
     *
     * <p>This is the {@code @Critical} variant: {@code buf} and {@code outBuf} may be
     * heap-backed segments (e.g. {@code MemorySegment.ofArray(byte[])}), avoiding off-heap
     * buffer copies entirely.
     *
     * @return 0 on success, non-zero simdjson error code on failure, or -2 if output exceeds capacity
     */
    @Function("es_stage1_run")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    int stage1(
        MemorySegment ctx,
        @OffsetSegment(offset = "offset", length = "len") MemorySegment buf,
        int offset,
        int len,
        @VectorSegment(countParam = "outBufCapacity", elementBits = 32) MemorySegment outBuf,
        int outBufCapacity,
        MemorySegment outCount
    );

    /**
     * Returns a pointer to a static, null-terminated error message for an error code returned by
     * {@link #stage1}. The caller must read the string from the returned segment.
     */
    @Function("es_stage1_error_message")
    MemorySegment errorMessage(int errorCode);
}
