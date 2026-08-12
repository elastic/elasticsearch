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

import java.io.IOException;

/**
 * Encodes and decodes a {@link StringColumnLayout#PLAIN} block: {@code [VInt length][bytes]} per value,
 * concatenated. For typical field-length distributions this is within about a byte per value of the
 * uncompressed minimum; block compression is a separate, later stage applied by the substrate's
 * {@code BlockBytesCodec}.
 *
 * <p><b>Flat-buffer block shape.</b> Both directions work on a pair of caller-owned scratches rather than
 * one array per value, so the hot path allocates nothing:
 * <ul>
 *   <li>{@code valueBytes} — the concatenated value bytes; value {@code i} occupies
 *       {@code valueBytes[valueOffsets[i], valueOffsets[i + 1])}.</li>
 *   <li>{@code valueOffsets} — {@code valueCount + 1} entries, the last marking the end of the last value.</li>
 * </ul>
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
     * {@code valueOffsets[0..valueCount]}. {@code valueBytes} must hold the block's total value bytes; the
     * column's {@code maxBlockValueBytes} sizes it.
     *
     * @return the total number of value bytes read
     */
    static int decode(DataInput in, int valueCount, byte[] valueBytes, int[] valueOffsets) throws IOException {
        int position = 0;
        valueOffsets[0] = 0;
        for (int i = 0; i < valueCount; i++) {
            int length = in.readVInt();
            in.readBytes(valueBytes, position, length);
            position += length;
            valueOffsets[i + 1] = position;
        }
        return position;
    }
}
