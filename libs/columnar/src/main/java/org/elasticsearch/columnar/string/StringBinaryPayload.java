/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Wire format for a document's string value(s) in a {@code BinaryDocValues} payload:
 * {@code [VInt count]} followed by {@code [VInt length][bytes]} per value, in written order.
 *
 * <p>Unlike {@code NumericBinaryPayload} the count cannot be derived from the payload length, because the
 * values are variable width — hence the explicit prefix. Values are never reordered.
 */
public final class StringBinaryPayload {

    /** Worst-case bytes a vint takes for a non-negative int; Lucene caps at 5 (5 * 7 bits covers an int). */
    private static final int MAX_VINT_BYTES = 5;

    private StringBinaryPayload() {}

    /** Packs {@code values[0..count)} into {@code out}, reusing its buffer. */
    public static BytesRef encode(BytesRef[] values, int count, BytesRefBuilder out) {
        int totalValueBytes = 0;
        for (int i = 0; i < count; i++) {
            totalValueBytes += values[i].length;
        }
        out.clear();
        out.grow(MAX_VINT_BYTES + count * MAX_VINT_BYTES + totalValueBytes);
        byte[] bytes = out.bytes();
        int position = writeVInt(bytes, 0, count);
        for (int i = 0; i < count; i++) {
            BytesRef value = values[i];
            position = writeVInt(bytes, position, value.length);
            System.arraycopy(value.bytes, value.offset, bytes, position, value.length);
            position += value.length;
        }
        out.setLength(position);
        return out.get();
    }

    /** Inline vint write; returns the next position. */
    private static int writeVInt(byte[] bytes, int position, int value) {
        while ((value & ~0x7F) != 0) {
            bytes[position++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        bytes[position++] = (byte) value;
        return position;
    }

    /**
     * A reusable cursor over one payload's values. {@link #reset} returns the value count; each
     * {@link #next()} yields the following value as a {@link BytesRef} pointing into the payload's own
     * backing array — valid only until the payload is replaced.
     */
    public static final class Reader {

        private final ByteArrayDataInput in = new ByteArrayDataInput();
        private final BytesRef value = new BytesRef();
        private int count;
        private int consumed;

        /** Positions on {@code payload} and returns how many values it holds. */
        public int reset(BytesRef payload) {
            in.reset(payload.bytes, payload.offset, payload.length);
            value.bytes = payload.bytes;
            count = in.readVInt();
            consumed = 0;
            return count;
        }

        /**
         * The next value; call exactly as many times as {@link #reset} reported.
         */
        public BytesRef next() {
            assert consumed < count : "next() called " + (consumed + 1) + " time(s) on a payload holding " + count + " value(s)";
            consumed++;
            int length = in.readVInt();
            value.offset = in.getPosition();
            value.length = length;
            in.skipBytes(length);
            return value;
        }
    }
}
