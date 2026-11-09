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
import org.elasticsearch.columnar.substrate.internal.ByteArrayInts;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Wire format for a document's numeric slots in a {@code BinaryDocValues} payload:
 *
 * <pre>
 * [vint slotCount][vint nullCount]
 * [ceil(slotCount / 8) bytes]            a bitmap of the null slots, only when nullCount &gt; 0
 * (slotCount - nullCount) x [8 bytes]    the non-null values, little-endian longs, in slot order
 * </pre>
 *
 * <p>Doubles are carried as longs, and slots are never reordered, so the payload keeps a document's array in the
 * order it was written, nulls included. A count of zero is an empty array, which is distinct from a document
 * without the field: that writes no payload at all.
 *
 * <p>The payload only carries values between the mapper and the codec, and back out of the codec's
 * {@code binaryValue()}: the column stores the values themselves, never the payload.
 */
public final class NumericBinaryPayload {

    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN)
        .withInvokeExactBehavior();

    private NumericBinaryPayload() {}

    private static int bitmapLength(int slotCount) {
        return (slotCount + Byte.SIZE - 1) / Byte.SIZE;
    }

    /**
     * Builds one document's payload a slot at a time. Reusable: {@link #build()} hands back the payload and
     * {@link #reset()} starts the next one in the same buffers.
     */
    public static final class Builder {
        private long[] values = new long[8];
        private byte[] nullBits = new byte[1];
        private int slotCount;
        private int nullCount;
        private final BytesRefBuilder out = new BytesRefBuilder();

        /** Appends a value slot. */
        public void addValue(long value) {
            if (slotCount - nullCount == values.length) {
                values = ArrayUtil.grow(values);
            }
            values[slotCount - nullCount] = value;
            growNullBits();
            slotCount++;
        }

        /** Appends a null slot. */
        public void addNull() {
            growNullBits();
            nullBits[slotCount >>> 3] |= (byte) (1 << (slotCount & 7));
            slotCount++;
            nullCount++;
        }

        private void growNullBits() {
            if ((slotCount >>> 3) >= nullBits.length) {
                // The grown tail is zeroed, so new slots start out non-null.
                nullBits = ArrayUtil.grow(nullBits, (slotCount >>> 3) + 1);
            }
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
            final int bitmapLength = nullCount > 0 ? bitmapLength(slotCount) : 0;
            out.grow(2 * ByteArrayInts.MAX_VINT_BYTES + bitmapLength + valueCount * Long.BYTES);
            final byte[] bytes = out.bytes();
            int pos = ByteArrayInts.writeVInt(slotCount, bytes, 0);
            pos += ByteArrayInts.writeVInt(nullCount, bytes, pos);
            System.arraycopy(nullBits, 0, bytes, pos, bitmapLength);
            pos += bitmapLength;
            for (int i = 0; i < valueCount; i++) {
                LONG.set(bytes, pos, values[i]);
                pos += Long.BYTES;
            }
            out.setLength(pos);
            return out.get();
        }

        /** Empties the builder for the next document. */
        public void reset() {
            Arrays.fill(nullBits, 0, Math.min(nullBits.length, bitmapLength(slotCount)), (byte) 0);
            slotCount = 0;
            nullCount = 0;
        }
    }

    /**
     * Reads one payload. Reusable: {@link #reset} points it at the next payload.
     *
     * <p>A caller that needs the slots, nulls included, walks them with {@link #nextIsNull()} and {@link #nextValue()}. One
     * that only needs the values takes them all at once with {@link #values()}, since they sit back to back in the payload.
     */
    public static final class Decoder {
        private byte[] bytes;
        private int bitmapOffset;
        private int valueOffset;
        private int slotCount;
        private int nullCount;
        private int slot;
        private int value;
        /** Holds the values {@link #values()} hands out; grown as needed, never shrunk. */
        private long[] valueBuffer = new long[8];

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
            bitmapOffset = pos[0];
            valueOffset = bitmapOffset + (nullCount > 0 ? bitmapLength(slotCount) : 0);
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
         * The payload's non-null values in the order they were written, in the first {@link #valueCount()} entries of the
         * returned array. The array belongs to this decoder and is only valid until the next {@link #reset}.
         */
        public long[] values() {
            final int count = valueCount();
            if (valueBuffer.length < count) {
                valueBuffer = new long[ArrayUtil.oversize(count, Long.BYTES)];
            }
            for (int i = 0; i < count; i++) {
                valueBuffer[i] = (long) LONG.get(bytes, valueOffset + i * Long.BYTES);
            }
            return valueBuffer;
        }

        /** Whether the next slot {@link #nextValue()} would read is null; advances past it if so. */
        public boolean nextIsNull() {
            assert slot < slotCount : "read past the last slot";
            if (nullCount > 0 && (bytes[bitmapOffset + (slot >>> 3)] & (1 << (slot & 7))) != 0) {
                slot++;
                return true;
            }
            return false;
        }

        /** Reads the next slot, which must not be null: call {@link #nextIsNull()} first. */
        public long nextValue() {
            assert slot < slotCount : "read past the last slot";
            final long v = (long) LONG.get(bytes, valueOffset + value * Long.BYTES);
            value++;
            slot++;
            return v;
        }
    }
}
