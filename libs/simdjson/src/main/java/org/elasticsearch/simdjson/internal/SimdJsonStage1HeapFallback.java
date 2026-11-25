/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * JDK 21 {@code @Critical} fallback for {@link SimdJsonLibrary#stage1}.
 *
 * <p>On JDK 21 the raw downcall handle is linked without {@code Linker.Option.critical(true)},
 * so it rejects heap-backed {@link MemorySegment} arguments. The annotation processor wires
 * {@code @Critical(fallbackAdapter = SimdJsonStage1HeapFallback.class)} so that on JDK 21 the
 * binding stages heap segments into a confined {@link Arena}, invokes the raw handle with
 * native segments, then copies outputs back. On JDK 22+ this class is not referenced.
 *
 * <p>When the input buffer is heap-backed, the {@code [offset, offset+len)} slice is copied to
 * native memory and stage 1 is run with {@code offset == 0}. Structural indices written to the
 * output buffer are shifted by the original {@code offset} on copy-back so they remain absolute
 * positions in the caller's buffer.
 */
public final class SimdJsonStage1HeapFallback {

    private static final ValueLayout.OfInt JAVA_INT = ValueLayout.JAVA_INT;

    private SimdJsonStage1HeapFallback() {}

    public static int stage1(
        MethodHandle mh,
        MemorySegment ctx,
        MemorySegment buf,
        int offset,
        int len,
        MemorySegment outBuf,
        int outBufCapacity,
        MemorySegment outCount
    ) throws Throwable {
        if (buf.isNative() && outBuf.isNative() && outCount.isNative()) {
            return (int) mh.invokeExact(ctx, buf, offset, len, outBuf, outBufCapacity, outCount);
        }
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment inputBuf;
            final int inputOffset;
            final boolean adjustIndices;
            if (buf.isNative()) {
                inputBuf = buf;
                inputOffset = offset;
                adjustIndices = false;
            } else {
                inputBuf = arena.allocate(len);
                MemorySegment.copy(buf, offset, inputBuf, 0, len);
                inputOffset = 0;
                adjustIndices = offset != 0;
            }

            MemorySegment nativeOut = outBuf.isNative() ? outBuf : arena.allocate((long) outBufCapacity * Integer.BYTES);
            MemorySegment nativeCount = outCount.isNative() ? outCount : arena.allocate(Integer.BYTES);

            int err = (int) mh.invokeExact(ctx, inputBuf, inputOffset, len, nativeOut, outBufCapacity, nativeCount);

            if (outCount.isNative() == false) {
                MemorySegment.copy(nativeCount, 0, outCount, 0, Integer.BYTES);
            }
            if (outBuf.isNative() == false) {
                int count = nativeCount.get(JAVA_INT, 0);
                if (err == 0 && count > 0) {
                    MemorySegment.copy(nativeOut, 0, outBuf, 0, (long) count * Integer.BYTES);
                    if (adjustIndices) {
                        for (int i = 0; i < count; i++) {
                            long indexOffset = (long) i * Integer.BYTES;
                            outBuf.set(JAVA_INT, indexOffset, outBuf.get(JAVA_INT, indexOffset) + offset);
                        }
                    }
                }
            }
            return err;
        }
    }
}
