/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * Creates {@link ByteBuffer}s over native memory that something else owns, such as a thread-confined {@link java.lang.foreign.Arena}.
 * This lives here because {@link MemorySegment#reinterpret(long)} is a restricted method, which only a module with native access may call.
 */
public final class SegmentBuffers {

    private SegmentBuffers() {}

    /**
     * Returns a {@link ByteBuffer} over {@code length} bytes of native memory at {@code address}. It is not bound to any arena, so it has
     * no owner-thread or liveness check and any thread may use it.
     *
     * <p>This is deliberately unsafe: if the memory is freed while the buffer is still reachable, using it reads or writes freed memory
     * instead of failing. Only components that enforce the memory's lifetime themselves should use it, and only hand the buffer out for as
     * long as they can guarantee that, for example by waiting for every user of the buffer before the memory is freed. If you have a
     * {@link MemorySegment}, use {@link MemorySegment#asByteBuffer()} instead.
     *
     * @param address address of the first byte, in memory that stays allocated for as long as the buffer is used
     * @param length the number of bytes in the buffer
     * @throws IllegalArgumentException if {@code address} is not positive or {@code length} is negative
     */
    public static ByteBuffer unscopedByteBuffer(long address, int length) {
        if (address <= 0 || length < 0) {
            throw new IllegalArgumentException("invalid native range [address=" + address + ", length=" + length + "]");
        }
        return MemorySegment.ofAddress(address).reinterpret(length).asByteBuffer();
    }
}
