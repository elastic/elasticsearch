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
 * get any other way: {@code addBinaryField(field, valuesProducer)} sees a single binary field at flush, so
 * the {@code <field>.counts} companion the mapper's other encodings lean on is out of reach and a blob whose
 * lone value is stored raw is indistinguishable from one value of the whole blob. Biasing a slot's length by
 * one leaves {@code 0} free to mean {@code null}, which keeps an inline null distinguishable from an empty
 * string.
 *
 * <p>Slots are never reordered, and a document with zero non-null values writes no payload at all — the
 * shape the mapper carries in {@code .counts} alone.
 *
 * <p>{@link LegacyEncoder} goes the other way, re-encoding a document's slots into the framing the mapper
 * would have written, which is what lets a column serve readers that still consult {@code .counts}.
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

    /**
     * Which of the mapper's formats a field is written in — both the payload the codec is handed at ingest and
     * the framing it re-encodes into on the way out. All three store a lone slot raw; they differ in whether a
     * length is biased, which is the same thing as whether the framing can express a null.
     */
    public enum Framing {
        /** {@code [vint len][bytes]...}, no null slots. Written by {@code SeparateCount} fields. */
        SEPARATE_COUNT((byte) 0),
        /** {@code [vint len+1][bytes]...} with {@code [vint 0]} for a null. Written by array-order fields. */
        ARRAY_ORDER((byte) 1),
        /**
         * One value per document, its own bytes, with no count and no length prefix anywhere. A field declared
         * single-valued leaves a count nothing to say, so its payload needs no framing at all: the codec takes
         * the blob as the value, and hands the value back as the blob. That keeps such a field's write path
         * free of any re-encoding, including the zero-copy batch path.
         */
        PLAIN((byte) 2);

        private final byte id;

        Framing(byte id) {
            this.id = id;
        }

        /** Frozen id recorded in column metadata; never reuse or renumber one. */
        public byte id() {
            return id;
        }

        public static Framing forId(byte id) {
            return switch (id) {
                case 0 -> SEPARATE_COUNT;
                case 1 -> ARRAY_ORDER;
                case 2 -> PLAIN;
                default -> throw new IllegalArgumentException("unknown string framing id: " + id);
            };
        }

        /** Whether a document's payload carries its own slot count, and so can hold more than one slot. */
        public boolean isSelfDescribing() {
            return this != PLAIN;
        }

        /** The bias a slot's length carries under this framing. */
        int bias() {
            return this == ARRAY_ORDER ? SLOT_LENGTH_BIAS : 0;
        }
    }

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
            return remaining;
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
     * Re-encodes a document's slots into the framing the mapper would have written, so a reader that still
     * consults {@code .counts} sees exactly the bytes it expects. Reusable across documents.
     *
     * <p>A lone slot is handed back raw under both framings — which is why {@link #begin} needs the count
     * before the first slot arrives, and why a lone slot may not be {@code null} (a document with no non-null
     * value writes no payload at all).
     */
    public static final class LegacyEncoder {

        private final BytesRefBuilder blob = new BytesRefBuilder();
        private int pos;
        private boolean raw;
        private int bias;

        /** Starts a document holding {@code slotCount} slots. */
        public void begin(Framing framing, int slotCount) {
            assert slotCount >= 1 : "a document with no slots writes no binary value";
            assert framing.isSelfDescribing() || slotCount == 1 : "a plain field holds one value per document";
            blob.clear();
            pos = 0;
            raw = slotCount == 1;
            bias = framing.bias();
        }

        /** Appends one slot; {@code null} denotes a null slot. */
        public void append(BytesRef value) {
            assert value != null || bias == SLOT_LENGTH_BIAS : "null slot under a framing that cannot express one";
            if (raw) {
                assert value != null : "a lone null slot writes no binary value";
                blob.grow(value.length);
                System.arraycopy(value.bytes, value.offset, blob.bytes(), 0, value.length);
                pos = value.length;
                return;
            }
            final int valueLength = value == null ? 0 : value.length;
            blob.grow(pos + VINT_MAX_BYTES + valueLength);
            final byte[] buffer = blob.bytes();
            pos = putVInt(buffer, value == null ? 0 : valueLength + bias, pos);
            if (value != null) {
                System.arraycopy(value.bytes, value.offset, buffer, pos, valueLength);
                pos += valueLength;
            }
        }

        /** The finished blob; valid until the next {@link #begin}. */
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
