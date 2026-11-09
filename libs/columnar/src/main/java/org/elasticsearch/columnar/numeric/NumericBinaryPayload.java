/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.substrate.internal.ByteArrayInts;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Wire format for a document's numeric slots in a {@code BinaryDocValues} payload:
 *
 * <pre>
 * [vint slotCount][vint nullCount]
 * [ceil(slotCount / 64) x 8 bytes]       the null slots, the words of a {@link FixedBitSet} as little-endian longs,
 *                                        only when nullCount &gt; 0
 * (slotCount - nullCount) x [8 bytes]    the non-null values, little-endian longs, in slot order
 * </pre>
 *
 * <p>Doubles are carried as longs, and slots are never reordered, so the payload keeps a document's array in the
 * order it was written, nulls included. A count of zero is an empty array, which is distinct from a document
 * without the field: that writes no payload at all.
 *
 * <p>The payload only carries values between the mapper and the codec, and back out of the codec's
 * {@code binaryValue()}: the column stores the values themselves, never the payload. So the null bitmap is laid out
 * as whole words, which a {@link FixedBitSet} reads and writes as they are, rather than packed into bytes.
 */
public final class NumericBinaryPayload {

    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN)
        .withInvokeExactBehavior();

    private NumericBinaryPayload() {}

    /** The number of bytes the null bitmap of a payload with {@code slotCount} slots and {@code nullCount} nulls takes. */
    private static int bitmapLength(int slotCount, int nullCount) {
        return nullCount > 0 ? FixedBitSet.bits2words(slotCount) * Long.BYTES : 0;
    }

    /** Writes {@code src[0..count)} as little-endian longs at {@code pos}, returning the position after them. */
    private static int writeLongs(long[] src, int count, byte[] dst, int pos) {
        for (int i = 0; i < count; i++) {
            LONG.set(dst, pos, src[i]);
            pos += Long.BYTES;
        }
        return pos;
    }

    /** Reads {@code count} little-endian longs at {@code pos} into {@code dst[0..count)}. */
    private static void readLongs(byte[] src, int pos, long[] dst, int count) {
        for (int i = 0; i < count; i++) {
            dst[i] = (long) LONG.get(src, pos + i * Long.BYTES);
        }
    }

    /**
     * Builds one document's payload a slot at a time. Reusable: {@link #build()} hands back the payload and
     * {@link #reset()} starts the next one in the same buffers.
     */
    public static final class Builder {
        private long[] values = new long[8];
        /** Which slots are null; holds no bit at or past {@link #slotCount}. */
        private FixedBitSet nullSlots = new FixedBitSet(Long.SIZE);
        private int slotCount;
        private int nullCount;
        private final BytesRefBuilder out = new BytesRefBuilder();

        /** Appends a value slot. */
        public void addValue(long value) {
            if (slotCount - nullCount == values.length) {
                values = ArrayUtil.grow(values);
            }
            values[slotCount - nullCount] = value;
            slotCount++;
        }

        /** Appends a null slot. */
        public void addNull() {
            nullSlots = FixedBitSet.ensureCapacity(nullSlots, slotCount);
            nullSlots.set(slotCount);
            slotCount++;
            nullCount++;
        }

        /**
         * The payload for {@code values[0..count)}, none of them null, in place of whatever the builder held. The returned
         * bytes belong to this builder and are only valid until it is next changed.
         */
        public BytesRef encode(long[] values, int count) {
            reset();
            if (this.values.length < count) {
                this.values = new long[ArrayUtil.oversize(count, Long.BYTES)];
            }
            System.arraycopy(values, 0, this.values, 0, count);
            slotCount = count;
            return build();
        }

        /** The number of slots appended so far. */
        public int slotCount() {
            return slotCount;
        }

        /**
         * The payload for the slots appended so far. The returned bytes belong to this builder and are only
         * valid until it is next changed.
         */
        public BytesRef build() {
            final int valueCount = slotCount - nullCount;
            final int bitmapLength = bitmapLength(slotCount, nullCount);
            if (bitmapLength > 0) {
                // The bitmap covers every slot, including values appended after the last null, which didn't grow it.
                nullSlots = FixedBitSet.ensureCapacity(nullSlots, slotCount);
            }
            out.grow(2 * ByteArrayInts.MAX_VINT_BYTES + bitmapLength + valueCount * Long.BYTES);
            final byte[] bytes = out.bytes();
            int pos = ByteArrayInts.writeVInt(slotCount, bytes, 0);
            pos += ByteArrayInts.writeVInt(nullCount, bytes, pos);
            pos = writeLongs(nullSlots.getBits(), bitmapLength / Long.BYTES, bytes, pos);
            pos = writeLongs(values, valueCount, bytes, pos);
            out.setLength(pos);
            return out.get();
        }

        /** Empties the builder for the next document. */
        public void reset() {
            if (nullCount > 0) {
                nullSlots.clear();
            }
            slotCount = 0;
            nullCount = 0;
        }
    }

    /**
     * Reads one payload. Reusable: {@link #reset} points it at the next payload.
     *
     * <p>A caller that needs the slots, nulls included, walks them with {@link #nextIsNull()} and {@link #nextValue()}. One
     * that only needs the values reads {@link #valueCount()} of them with {@link #nextValue()} alone.
     */
    public static final class Decoder {
        private byte[] bytes;
        /** The current payload's null slots, read in by {@link #reset} when it has any; bits past its slots are stale. */
        private FixedBitSet nullSlots = new FixedBitSet(Long.SIZE);
        private int valueOffset;
        private int slotCount;
        private int nullCount;
        private int slot;
        private int value;

        /**
         * Points the decoder at {@code payload}, which must stay unchanged while it is read.
         *
         * @return the number of slots, nulls included
         */
        public int reset(BytesRef payload) throws IOException {
            final int[] pos = { payload.offset };
            bytes = payload.bytes;
            slotCount = ByteArrayInts.readVInt(bytes, pos);
            nullCount = ByteArrayInts.readVInt(bytes, pos);
            final int bitmapLength = bitmapLength(slotCount, nullCount);
            if (bitmapLength > 0) {
                nullSlots = FixedBitSet.ensureCapacity(nullSlots, slotCount);
                readLongs(bytes, pos[0], nullSlots.getBits(), bitmapLength / Long.BYTES);
            }
            valueOffset = pos[0] + bitmapLength;
            slot = 0;
            value = 0;
            return slotCount;
        }

        /** The number of null slots in the payload. */
        public int nullSlotCount() {
            return nullCount;
        }

        /** The number of non-null values in the payload: its slots less its nulls. */
        public int valueCount() {
            return slotCount - nullCount;
        }

        /**
         * Moves to the next slot and says whether it is null. A caller walking the slots asks this once per slot, and reads
         * the slot's value with {@link #nextValue()} when it isn't null.
         */
        public boolean nextIsNull() {
            assert slot < slotCount : "read past the last slot";
            // Each value read in a slot walk is a non-null slot already passed. More values than slots means values were
            // read without walking their slots, and this slot is not the one after the last value read.
            assert slot >= value : "slot " + slot + " is behind value " + value + ": a slot walk can't follow values-only reads";
            final int current = slot++;
            return nullCount > 0 && nullSlots.get(current);
        }

        /**
         * Reads the next non-null value. The values sit apart from the null slots, so this never looks at them: a caller that
         * only wants the values calls this {@link #valueCount()} times, and one walking the slots calls it for each slot
         * {@link #nextIsNull()} says isn't null.
         */
        public long nextValue() {
            assert value < valueCount() : "read past the last value";
            return (long) LONG.get(bytes, valueOffset + value++ * Long.BYTES);
        }
    }
}
