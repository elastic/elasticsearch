/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.UninitializedArrays;

/**
 * Collects one batch of non-null KEYWORD values as a flat {@code byte[]} plus {@code int[]} start offsets: the
 * storage {@link BlockFactory#newDirectBytesRefVector} wraps as-is. PLAIN BINARY payloads are decoded straight
 * into the tail of {@link #bytes()}, so the one copy out of the Parquet page is also the vector's storage, and
 * the batch never pays {@link BytesRefArray#append(BytesRef)} per value. That per-value append costs 12-20%
 * end to end on wide KEYWORD scans, and this avoids it without changing {@link BytesRefArray} itself.
 * <p>
 * The flat arrays are plain heap arrays that the breaker sees only once the vector is built. So a batch only
 * stays here while bytes plus offsets fit {@link BlockFactory#maxPrimitiveArrayBytes()}, the same rule ES|QL
 * block builders use to choose plain arrays over {@code BigArrays}. A batch that grows past it, or that the
 * caller cannot keep here for another reason, is moved into a {@link BytesRefArray} with {@link #appendTo}
 * and continues on recycled, breaker-accounted pages.
 * <p>
 * Usage per chunk: {@link #reserve} room for the chunk's payload bytes, decode into {@link #bytes()} at the
 * returned offset, then {@link #commit} the chunk's value ends. A chunk that is reserved and decoded but not
 * committed leaves the accumulator unchanged, so while it is empty the tail doubles as a scratch buffer.
 */
final class FlatBytesRefAccumulator {
    private final int maxValues;
    private final long maxBytes;
    private byte[] bytes = BytesRef.EMPTY_BYTES;
    /** {@code offsets[i]} is the start of value {@code i}; {@code offsets[count]} is {@link #length}. Allocated on first commit. */
    private int[] offsets;
    private int count;
    private int length;

    /**
     * @param maxValues the most values one batch can hold; the offsets array is sized for this up front
     * @param maxBytes  the most bytes plus offsets the flat arrays may hold, normally {@link BlockFactory#maxPrimitiveArrayBytes()}
     */
    FlatBytesRefAccumulator(int maxValues, long maxBytes) {
        this.maxValues = maxValues;
        this.maxBytes = Math.min(maxBytes, ArrayUtil.MAX_ARRAY_LENGTH);
    }

    int count() {
        return count;
    }

    boolean isEmpty() {
        return count == 0;
    }

    /** Payload bytes of the committed values. */
    int length() {
        return length;
    }

    /** Storage the next chunk is decoded into, at the offset {@link #reserve} returned. Re-read after every {@code reserve}. */
    byte[] bytes() {
        return bytes;
    }

    /**
     * Whether {@code extraBytes} more payload still fits under the cap, counting the offsets array at its full
     * {@code maxValues + 1} capacity since that is what the built vector holds and charges for.
     */
    boolean fits(long extraBytes) {
        return length + extraBytes + (maxValues + 1L) * Integer.BYTES <= maxBytes;
    }

    /**
     * Makes room for {@code extraBytes} after the committed values and returns the offset to decode them at.
     * <p>
     * While nothing is committed the array grows to exactly the size needed: there is nothing to copy, and most
     * batches live in a single Parquet page, so the common case leaves no slack in the built vector. Growth past
     * committed bytes is geometric, since a batch that spans many small pages would otherwise recopy every
     * committed byte once per page, which is quadratic. Geometric growth is clamped to the cap ({@code maxBytes}
     * minus the offsets array): near it, the array grows to the cap itself, which can be more than this reserve
     * needs but never more than {@link #fits} allows. Any slack left over is charged with the built vector, whose
     * {@code ramBytesUsed} counts the array's capacity.
     */
    int reserve(int extraBytes) {
        int needed = Math.addExact(length, extraBytes);
        if (needed > bytes.length) {
            int newSize = needed;
            if (length > 0) {
                long cap = Math.max(needed, maxBytes - (maxValues + 1L) * Integer.BYTES);
                newSize = (int) Math.min(ArrayUtil.oversize(needed, Byte.BYTES), cap);
            }
            byte[] grown = UninitializedArrays.newByteArray(newSize);
            System.arraycopy(bytes, 0, grown, 0, length);
            bytes = grown;
        }
        return length;
    }

    /**
     * Commits {@code n} values decoded at the {@link #reserve reserved} tail. {@code exclusiveEnds[i]} is the
     * exclusive end of value {@code i} relative to that tail, as {@link PlainValueDecoder#copyPlainPayloads} writes.
     */
    void commit(int[] exclusiveEnds, int n) {
        if (n == 0) {
            return;
        }
        if (offsets == null) {
            offsets = UninitializedArrays.newIntArray(maxValues + 1);
            offsets[0] = 0;
        }
        assert count + n <= maxValues : count + " + " + n + " > " + maxValues;
        int base = length;
        for (int i = 0; i < n; i++) {
            offsets[count + 1 + i] = base + exclusiveEnds[i];
        }
        count += n;
        length = offsets[count];
        assert length <= bytes.length : length + " > " + bytes.length;
    }

    /**
     * Commits {@code times} copies of {@code constant} followed by the {@code n} values decoded at offset 0 of an
     * empty accumulator. This materializes a constant run the caller tracked implicitly once a chunk breaks it.
     * The caller checks {@link #fits} for the run plus the chunk first.
     */
    void commitAfterConstantRun(BytesRef constant, int times, int[] exclusiveEnds, int n) {
        assert count == 0 && length == 0 : "a constant run must lead the batch";
        int chunkBytes = n == 0 ? 0 : exclusiveEnds[n - 1];
        byte[] chunk = bytes;
        int runBytes = Math.multiplyExact(times, constant.length);
        bytes = UninitializedArrays.newByteArray(Math.addExact(runBytes, chunkBytes));
        int[] runEnds = new int[times];
        for (int i = 0; i < times; i++) {
            System.arraycopy(constant.bytes, constant.offset, bytes, i * constant.length, constant.length);
            runEnds[i] = (i + 1) * constant.length;
        }
        commit(runEnds, times);
        System.arraycopy(chunk, 0, bytes, runBytes, chunkBytes);
        commit(exclusiveEnds, n);
    }

    /** Appends every committed value to {@code array}, one at a time; used once, when a batch leaves the flat path. */
    void appendTo(BytesRefArray array) {
        BytesRef scratch = new BytesRef();
        scratch.bytes = bytes;
        for (int i = 0; i < count; i++) {
            scratch.offset = offsets[i];
            scratch.length = offsets[i + 1] - scratch.offset;
            array.append(scratch);
        }
    }

    /** Appends every committed value to {@code builder}; used once, when an optional batch meets its first null. */
    void appendTo(BytesRefBlock.Builder builder) {
        BytesRef scratch = new BytesRef();
        scratch.bytes = bytes;
        for (int i = 0; i < count; i++) {
            scratch.offset = offsets[i];
            scratch.length = offsets[i + 1] - scratch.offset;
            builder.appendBytesRef(scratch);
        }
    }

    /**
     * Wraps the committed values without copying. The vector keeps the arrays, so the accumulator must not be
     * used afterwards. Charges the breaker for the arrays' full capacity.
     */
    BytesRefVector build(BlockFactory blockFactory) {
        assert count > 0 : "an empty batch has no flat storage";
        return blockFactory.newDirectBytesRefVector(bytes, offsets, count);
    }
}
