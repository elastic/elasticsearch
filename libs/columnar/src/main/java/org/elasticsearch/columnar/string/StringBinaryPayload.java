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
import org.elasticsearch.columnar.substrate.internal.ByteArrayInts;

import java.io.IOException;
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

    /** Added to a slot's length before it is written, leaving {@code 0} free to mean {@code null}. */
    public static final int SLOT_LENGTH_BIAS = 1;

    /** The payload of a document holding no slots at all: a count of zero and nothing after it. */
    public static final BytesRef EMPTY = new BytesRef(new byte[] { 0 });

    private StringBinaryPayload() {}

    /**
     * Encodes {@code slots} in document order, a {@code null} element denoting a null slot, into a payload of
     * its own. Callers encoding a document per iteration use a {@link Builder} instead — it reuses its buffer
     * where this allocates one.
     */
    public static BytesRef encode(Collection<BytesRef> slots) {
        final Builder builder = new Builder();
        for (BytesRef slot : slots) {
            builder.appendSlot(slot);
        }
        return BytesRef.deepCopyOf(builder.build());
    }

    /**
     * Builds one document's payload, a slot at a time. Reusable through {@link #reset}, so a caller encoding a
     * whole segment allocates one buffer rather than one per document.
     *
     * <p>The count belongs in front of the slots but is only known once they have all arrived. Rather than
     * buffer the slots elsewhere or shuffle them along afterwards, the builder leaves a vint's worth of room
     * ahead of them and writes the count so that it ends exactly where the first slot begins. That is why
     * {@link #build} hands back a {@link BytesRef} whose offset is not zero.
     */
    public static final class Builder {

        /** Room left for the count, which {@link #build} writes back into once the slots have all arrived. */
        private static final int COUNT_RESERVE = ByteArrayInts.MAX_VINT_BYTES;

        private final BytesRefBuilder blob = new BytesRefBuilder();
        private final BytesRef payload = new BytesRef();
        private int pos = COUNT_RESERVE;
        private int slotCount;

        public Builder() {
            blob.grow(COUNT_RESERVE);
        }

        /** Appends one slot to the document under construction; {@code null} denotes a null slot. */
        public void appendSlot(BytesRef value) {
            final int valueLength = value == null ? 0 : value.length;
            // grow (not growNoCopy): the slots already appended have to survive.
            blob.grow(pos + ByteArrayInts.MAX_VINT_BYTES + valueLength);
            final byte[] buffer = blob.bytes();
            pos += ByteArrayInts.writeVInt(value == null ? 0 : valueLength + SLOT_LENGTH_BIAS, buffer, pos);
            if (value != null) {
                System.arraycopy(value.bytes, value.offset, buffer, pos, valueLength);
                pos += valueLength;
            }
            slotCount++;
        }

        /**
         * The finished payload, count and all. Points into this builder's own buffer, so it is valid until the
         * next {@link #reset}; a caller that needs to keep it must copy it out.
         */
        public BytesRef build() {
            final int start = COUNT_RESERVE - ByteArrayInts.vIntLength(slotCount);
            ByteArrayInts.writeVInt(slotCount, blob.bytes(), start);
            payload.bytes = blob.bytes();
            payload.offset = start;
            payload.length = pos - start;
            return payload;
        }

        /** Drops what has been appended and starts a new document, keeping the buffer. */
        public void reset() {
            pos = COUNT_RESERVE;
            slotCount = 0;
        }
    }

    /**
     * Reads a payload one slot at a time. Reusable across documents, and {@link #next} points into the
     * payload's own bytes rather than copying, so a returned {@link BytesRef} lives exactly as long as the
     * payload does.
     */
    public static final class Decoder {

        private final BytesRef scratch = new BytesRef();
        /** Held as a field and reused, so walking a payload allocates nothing. */
        private final int[] cursor = new int[1];
        private final int[] scanCursor = new int[1];
        private byte[] bytes;
        private int start;
        private int end;
        private int remaining;

        /** Positions at the first slot of {@code payload} and returns its slot count. */
        public int reset(BytesRef payload) throws IOException {
            this.bytes = payload.bytes;
            this.start = payload.offset;
            this.cursor[0] = payload.offset;
            this.end = payload.offset + payload.length;
            this.remaining = ByteArrayInts.readVInt(bytes, cursor);
            assert remaining > 0 || cursor[0] == end : "payload has " + (end - cursor[0]) + " trailing bytes";
            return remaining;
        }

        /**
         * The next slot, or {@code null} for a null slot. Call exactly as many times as {@link #reset}
         * returned.
         */
        public BytesRef next() throws IOException {
            assert remaining > 0 : "payload has no slot left";
            remaining--;
            final int encodedLength = ByteArrayInts.readVInt(bytes, cursor);
            if (encodedLength == 0) {
                assert remaining > 0 || cursor[0] == end : "payload has " + (end - cursor[0]) + " trailing bytes";
                return null;
            }
            scratch.bytes = bytes;
            scratch.offset = cursor[0];
            scratch.length = encodedLength - SLOT_LENGTH_BIAS;
            cursor[0] += scratch.length;
            assert remaining > 0 || cursor[0] == end : "payload has " + (end - cursor[0]) + " trailing bytes";
            return scratch;
        }

        /**
         * How many slots of the payload {@link #reset} was last given are null. Walks the lengths without
         * touching a value's bytes or disturbing the cursor, so a caller can ask before or during iteration.
         */
        public int nullSlotCount() throws IOException {
            scanCursor[0] = start;
            final int slots = ByteArrayInts.readVInt(bytes, scanCursor);
            int nulls = 0;
            for (int i = 0; i < slots; i++) {
                final int encodedLength = ByteArrayInts.readVInt(bytes, scanCursor);
                if (encodedLength == 0) {
                    nulls++;
                } else {
                    scanCursor[0] += encodedLength - SLOT_LENGTH_BIAS;
                }
            }
            assert scanCursor[0] == end : "payload has " + (end - scanCursor[0]) + " trailing bytes";
            return nulls;
        }
    }

}
