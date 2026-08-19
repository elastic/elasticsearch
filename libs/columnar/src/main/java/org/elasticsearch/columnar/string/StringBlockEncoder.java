/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;

/**
 * Encodes and decodes one block of string values as {@code [vint length][bytes]} per value.
 *
 * <p>Both sides work in a flat layout — the block's value bytes concatenated, plus the offset of each value
 * within them — so a decoded block hands out values without copying any of them again.
 *
 * <p>The number of values in a block is known from the column metadata, so no count is written per block.
 */
final class StringBlockEncoder {

    private StringBlockEncoder() {}

    /** Writes {@code valueCount} values held in the flat layout to {@code out}. */
    static void encode(byte[] valueBytes, int[] valueOffsets, int valueCount, DataOutput out) throws IOException {
        for (int i = 0; i < valueCount; i++) {
            int start = valueOffsets[i];
            int length = valueOffsets[i + 1] - start;
            out.writeVInt(length);
            out.writeBytes(valueBytes, start, length);
        }
    }

    /**
     * Reads {@code valueCount} values from {@code in} into the flat layout, filling
     * {@code valueOffsets[0..valueCount]}.
     *
     * <p>{@code valueBytes} is grown as the block is read rather than sized up front, so the caller does not
     * have to know a block's decoded length before decoding it — which no metadata field could tell it once a
     * codec compresses the block. A reused buffer settles after the first few blocks.
     *
     * @return the buffer holding the block's value bytes, which is {@code valueBytes} unless it had to grow
     */
    static byte[] decode(DataInput in, int valueCount, byte[] valueBytes, int[] valueOffsets) throws IOException {
        int position = 0;
        valueOffsets[0] = 0;
        for (int i = 0; i < valueCount; i++) {
            int length = in.readVInt();
            valueBytes = ArrayUtil.grow(valueBytes, position + length);
            in.readBytes(valueBytes, position, length);
            position += length;
            valueOffsets[i + 1] = position;
        }
        return valueBytes;
    }
}
