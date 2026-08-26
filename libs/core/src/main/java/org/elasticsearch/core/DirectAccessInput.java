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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * An optional interface that an IndexInput can implement to provide direct
 * access to the underlying data as a {@link MemorySegment}. This enables
 * zero-copy access to memory-mapped data for SIMD-accelerated vector scoring.
 *
 * <p>The memory segment is valid only for the duration of the caller-supplied action.
 * All ref-counting and resource releases, if any, are handled internally.
 */
public interface DirectAccessInput {

    /**
     * If a direct memory segment view is available for the given range, passes it
     * to {@code action} and returns {@code true}. Otherwise returns
     * {@code false} without invoking the action.
     *
     * <p>The memory segment is read-only and valid only for the duration of the
     * action. Callers must not retain references to it after the action returns.
     *
     * @param offset the byte offset within the input
     * @param length the number of bytes requested
     * @param action the action to perform with the memory segment
     * @return {@code true} if a segment was available and the action was invoked
     */
    boolean withMemorySegmentSlice(long offset, long length, CheckedConsumer<MemorySegment, IOException> action) throws IOException;

    /**
     * Bulk variant of {@link #withMemorySegmentSlice}. Resolves {@code count}
     * file ranges to raw native addresses, writes them into {@code addressesScratch[0..count)},
     * and invokes the {@code action} while all segments are valid.
     * All ref-counting and resource management is handled internally.
     *
     * <p>Addresses are written as raw 64-bit values to {@code addressesScratch} via
     * {@link java.lang.foreign.ValueLayout#JAVA_LONG} (pointer-width on 64-bit JVMs), avoiding
     * per-call {@link MemorySegment} slice allocations.
     *
     * <p>The addresses in {@code addressesScratch} are valid only for the duration of the action.
     * Callers must not read them after the action returns.
     *
     * @param offsets  file byte offsets for each range
     * @param length   byte length of each range (same for all)
     * @param count    number of ranges to resolve
     * @param addressesScratch pre-allocated output buffer; must hold at least {@code count} pointer-width
     *                 entries. May be larger and reused across calls; only {@code [0, count)} are written.
     *                 Its base address must be aligned to {@link java.lang.foreign.ValueLayout#ADDRESS}'s
     *                 byte alignment.
     * @param action   invoked with {@code addressesScratch}; only the first {@code count} address slots
     *                 contain valid data, and those addresses are valid only for the duration of the call
     * @return {@code true} if all ranges were resolved and the action was invoked; {@code false} otherwise
     */
    boolean withSliceAddresses(
        long[] offsets,
        int length,
        int count,
        MemorySegment addressesScratch,
        CheckedConsumer<MemorySegment, IOException> action
    ) throws IOException;

    /**
     * Validates the {@code offsets}, {@code count} and {@code addressesScratch} arguments for
     * {@link #withSliceAddresses}.
     * Returns {@code true} if count is zero (caller should treat as a no-op), {@code false} otherwise.
     */
    static boolean checkSlicesArgs(long[] offsets, int count, MemorySegment addressesScratch) {
        if (count < 0) {
            throw new IllegalArgumentException("count must not be negative, got [" + count + "]");
        }
        if (offsets.length < count) {
            throw new IllegalArgumentException("offsets array length [" + offsets.length + "] is less than count [" + count + "]");
        }
        if (addressesScratch.byteSize() < count * ValueLayout.ADDRESS.byteSize()) {
            throw new IllegalArgumentException(
                "addressesScratch segment byte size [" + addressesScratch.byteSize() + "] is too small to hold [" + count + "] pointers"
            );
        }
        boolean isAligned = addressesScratch.address() % ValueLayout.ADDRESS.byteAlignment() == 0;
        if (isAligned == false) {
            throw new IllegalArgumentException(
                "addressesScratch segment address ["
                    + addressesScratch.address()
                    + "] is not aligned to ["
                    + ValueLayout.ADDRESS.byteAlignment()
                    + "] bytes"
            );
        }
        return count == 0;
    }
}
