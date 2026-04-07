/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An optional interface that an IndexInput can implement to provide direct
 * access to the underlying data as a {@link ByteBuffer}. This enables
 * zero-copy access to memory-mapped data for SIMD-accelerated vector scoring.
 *
 * <p> The byte buffer is passed to the caller's action and is only valid for
 * the duration of that call. All ref-counting and resource releases, if any,
 * is handled internally.
 */
public interface DirectAccessInput {

    /**
     * If a direct byte buffer view is available for the given range, passes it
     * to {@code action} and returns {@code true}. Otherwise returns
     * {@code false} without invoking the action.
     *
     * <p>The byte buffer is read-only and valid only for the duration of the
     * action. Callers must not retain references to it after the action returns.
     *
     * @param offset the byte offset within the input
     * @param length the number of bytes requested
     * @param action the action to perform with the byte buffer
     * @return {@code true} if a buffer was available and the action was invoked
     */
    boolean withByteBufferSlice(long offset, long length, CheckedConsumer<ByteBuffer, IOException> action) throws IOException;

    /**
     * Bulk variant of {@link #withByteBufferSlice}. Resolves {@code count}
     * file ranges to direct byte buffers and invokes the action while all
     * buffers are valid. All ref-counting and resource management is handled
     * internally.
     *
     * <p> The byte buffers in the array passed to the action are read-only and
     * valid only for the duration of the action. Callers must not retain
     * references to them after the action returns.
     *
     * @param offsets file byte offsets for each range
     * @param length  byte length of each range (same for all)
     * @param count   number of ranges to resolve
     * @param action  receives a {@code ByteBuffer[]} where entry {@code i}
     *                corresponds to {@code offsets[i]}
     * @return {@code true} if all ranges were available and the action was
     *         invoked; {@code false} otherwise
     */
    boolean withByteBufferSlices(long[] offsets, int length, int count, CheckedConsumer<ByteBuffer[], IOException> action)
        throws IOException;

    /**
     * Validates the {@code offsets} and {@code count} arguments for
     * {@link #withByteBufferSlices}. Throws on negative count or an
     * undersized offsets array. Returns {@code true} if count is zero
     * (caller should treat as a no-op), {@code false} otherwise.
     */
    static boolean checkSlicesArgs(long[] offsets, int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count must not be negative, got " + count);
        }
        if (offsets.length < count) {
            throw new IllegalArgumentException("offsets array length " + offsets.length + " is less than count " + count);
        }
        return count == 0;
    }
}
