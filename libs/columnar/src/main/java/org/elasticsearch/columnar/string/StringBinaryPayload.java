/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.util.Collection;

/**
 * Wire format for a document's string slots in a {@code BinaryDocValues} payload:
 *
 * <pre>
 * [vint slotCount] then slotCount slots of:
 *     [vint len+1][bytes]   a value of length len   (len 0 -&gt; prefix 1, the empty string)
 *     [vint 0]              a null slot, no bytes follow
 * </pre>
 *
 * <p>The count sits <b>in</b> the payload because that is the one thing a {@code DocValuesConsumer} cannot
 * get any other way: {@code addBinaryField(field, valuesProducer)} sees a single binary field at flush, so a
 * companion count field is out of reach and a blob whose lone value is stored raw is indistinguishable from
 * one value that is the whole blob. Biasing a slot's length by one leaves {@code 0} free to mean {@code null},
 * which keeps an inline null distinguishable from an empty string.
 *
 * <p>Slots are never reordered. Because the count is carried, a document with no slots at all
 * ({@code [vint 0]}, an empty array) and one whose slots are all null are both expressible, so the format
 * needs no companion field to describe any shape a document can take.
 *
 * <p>This is the format both sides of the codec speak: the mapper writes it, the column is built from it at
 * flush, and {@link ColumnarStringBinaryDocValues#binaryValue} rebuilds it from the stored slots on read.
 */
public final class StringBinaryPayload {

    /** Bytes a vint can occupy; the width a slot's length prefix is reserved at before it is known. */
    public static final int VINT_MAX_BYTES = 5;

    /** Added to a slot's length before it is written, leaving {@code 0} free to mean {@code null}. */
    public static final int SLOT_LENGTH_BIAS = 1;

    /**
     * Bytes to leave in front of a document's first slot when the slots are appended before their count is
     * known. {@link #writeCountBefore} right-aligns the count against this reserve, so the payload needs no
     * shuffling at flush — it just starts at the offset that call returns.
     */
    public static final int COUNT_RESERVE = VINT_MAX_BYTES;

    /** The payload of a document holding no slots at all: a count of zero and nothing after it. */
    public static final BytesRef EMPTY = new BytesRef(new byte[] { 0 });

    private StringBinaryPayload() {}

    /**
     * Encodes {@code slots} in document order, a {@code null} element denoting a null slot. Used where the
     * whole document is already in hand; callers that append slot by slot use {@link #appendSlot} and
     * {@link #writeCountBefore} instead.
     */
    public static BytesRef encode(Collection<BytesRef> slots) {
        int byteCount = 0;
        for (BytesRef slot : slots) {
            if (slot != null) {
                byteCount += slot.length;
            }
        }
        BytesRefBuilder blob = new BytesRefBuilder();
        blob.grow(byteCount + (slots.size() + 1) * VINT_MAX_BYTES);
        int pos = putVInt(blob.bytes(), slots.size(), 0);
        for (BytesRef slot : slots) {
            pos = appendSlot(blob, pos, slot);
        }
        blob.setLength(pos);
        return BytesRef.deepCopyOf(blob.get());
    }

    /**
     * Appends one slot to a document blob under construction in {@code blob}, growing it as needed, and
     * returns the write position just past the slot. A {@code null} {@code value} denotes a null slot.
     */
    public static int appendSlot(BytesRefBuilder blob, int pos, BytesRef value) {
        final int valueLength = value == null ? 0 : value.length;
        // grow (not growNoCopy): earlier slots of this document must survive.
        blob.grow(pos + VINT_MAX_BYTES + valueLength);
        final byte[] buffer = blob.bytes();
        pos = putVInt(buffer, value == null ? 0 : valueLength + SLOT_LENGTH_BIAS, pos);
        if (value != null) {
            System.arraycopy(value.bytes, value.offset, buffer, pos, valueLength);
            pos += valueLength;
        }
        return pos;
    }

    /**
     * Writes {@code slotCount} so that it ends exactly at {@link #COUNT_RESERVE}, and returns the offset the
     * finished payload begins at. Lets a caller append slots from {@code COUNT_RESERVE} onwards while the
     * count is still unknown and close the document without moving any of them.
     */
    public static int writeCountBefore(BytesRefBuilder blob, int slotCount) {
        blob.grow(COUNT_RESERVE);
        final int start = COUNT_RESERVE - vIntLength(slotCount);
        putVInt(blob.bytes(), slotCount, start);
        return start;
    }

    /**
     * Reads a payload one slot at a time. Reusable across documents, and {@link #next} points into the
     * payload's own bytes rather than copying, so a returned {@link BytesRef} lives exactly as long as the
     * payload does.
     */
    public static final class Decoder {

        private final BytesRef scratch = new BytesRef();
        private byte[] bytes;
        private int start;
        private int pos;
        private int end;
        private int remaining;

        /** Positions at the first slot of {@code payload} and returns its slot count. */
        public int reset(BytesRef payload) {
            this.bytes = payload.bytes;
            this.start = payload.offset;
            this.pos = payload.offset;
            this.end = payload.offset + payload.length;
            this.remaining = readVInt();
            assert remaining > 0 || pos == end : "payload has " + (end - pos) + " trailing bytes";
            return remaining;
        }

        /**
         * The next slot, or {@code null} for a null slot. Call exactly as many times as {@link #reset}
         * returned.
         */
        public BytesRef next() {
            assert remaining > 0 : "payload has no slot left";
            remaining--;
            final int encodedLength = readVInt();
            if (encodedLength == 0) {
                assert remaining > 0 || pos == end : "payload has " + (end - pos) + " trailing bytes";
                return null;
            }
            scratch.bytes = bytes;
            scratch.offset = pos;
            scratch.length = encodedLength - SLOT_LENGTH_BIAS;
            pos += scratch.length;
            assert remaining > 0 || pos == end : "payload has " + (end - pos) + " trailing bytes";
            return scratch;
        }

        /**
         * How many slots of the payload {@link #reset} was last given are null. Walks the lengths without
         * touching a value's bytes or disturbing the cursor, so a caller can ask before or during iteration.
         */
        public int nullSlotCount() {
            int at = start;
            int slots = 0;
            int shift = 0;
            byte b;
            do {
                b = bytes[at++];
                slots |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            int nulls = 0;
            for (int i = 0; i < slots; i++) {
                int encodedLength = 0;
                shift = 0;
                do {
                    b = bytes[at++];
                    encodedLength |= (b & 0x7F) << shift;
                    shift += 7;
                } while ((b & 0x80) != 0);
                if (encodedLength == 0) {
                    nulls++;
                } else {
                    at += encodedLength - SLOT_LENGTH_BIAS;
                }
            }
            assert at == end : "payload has " + (end - at) + " trailing bytes";
            return nulls;
        }

        private int readVInt() {
            int value = 0;
            int shift = 0;
            byte b;
            do {
                assert pos < end : "payload ended mid-vint";
                b = bytes[pos++];
                value |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return value;
        }
    }

    /**
     * Builds a payload slot by slot when the count is known up front — the shape the column reads back in,
     * where a document's slot count comes from its value-address range. Reusable across documents.
     */
    public static final class Encoder {

        private final BytesRefBuilder blob = new BytesRefBuilder();
        private int pos;

        /** Starts a document holding {@code slotCount} slots. */
        public void begin(int slotCount) {
            blob.clear();
            blob.grow(VINT_MAX_BYTES);
            pos = putVInt(blob.bytes(), slotCount, 0);
        }

        /** Appends one slot; {@code null} denotes a null slot. */
        public void append(BytesRef value) {
            pos = appendSlot(blob, pos, value);
        }

        /** The finished payload; valid until the next {@link #begin}. */
        public BytesRef get() {
            blob.setLength(pos);
            return blob.get();
        }
    }

    // TODO: replace with the internal ByteArrayInts helper once #157431 lands.
    private static int putVInt(byte[] buffer, int value, int pos) {
        while ((value & ~0x7F) != 0) {
            buffer[pos++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buffer[pos++] = (byte) value;
        return pos;
    }

    // TODO: replace with the internal ByteArrayInts helper once #157431 lands.
    private static int vIntLength(int value) {
        int length = 1;
        while ((value & ~0x7F) != 0) {
            length++;
            value >>>= 7;
        }
        return length;
    }
}
