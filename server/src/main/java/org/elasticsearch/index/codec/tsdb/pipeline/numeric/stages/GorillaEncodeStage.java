/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadEncoder;

import java.io.IOException;

public final class GorillaEncodeStage implements PayloadEncoder {

    private static final int SCRATCHPAD_SIZE = 16;

    // NOTE: 6-bit leading-zeros field. The original Gorilla paper used 5 bits and
    // clamped to 0-31, which inflates meaningfulBits when XOR has >31 leading zeros.
    // With 6 bits we store the actual count (0-63) and avoid clamping, improving
    // compression for slowly-changing values.
    private static final int LEADING_ZEROS_BITS = 6;
    private static final int MEANINGFUL_BITS_BITS = 6;

    // NOTE: Scratchpad offsets for bit buffer state persisted across writeBits/readBits calls.
    private static final int BIT_BUFFER_OFFSET = 0;
    private static final int BITS_IN_BUFFER_OFFSET = 8;

    private final byte[] scratchpad = new byte[SCRATCHPAD_SIZE];

    @Override
    public byte id() {
        return StageId.GORILLA_PAYLOAD.id;
    }

    // NOTE: Payload layout: [valueCount: VInt] [first value: 64 raw bits]
    // followed by a Gorilla-encoded XOR bitstream. Each subsequent value is XORed
    // with its predecessor and encoded as one of three cases:
    // '0' -- XOR is zero (value unchanged). Fast path: single bit, no further reads.
    // '10' -- reuse previous window (prevLeadingZeros, prevMeaningfulBits).
    // Writes prevMeaningfulBits of the XOR shifted into the window.
    // '11' -- new window: [leadingZeros: 6 bits (0-63, no clamping)]
    // [meaningfulBits-1: 6 bits] [meaningful bits].
    // Trailing zeros are derived as 64 - leadingZeros - meaningfulBits.
    // Writes directly to DataOutput via the scratchpad bit buffer, not MetadataWriter.
    @Override
    public void encode(final long[] values, int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
        if (valueCount == 0) {
            out.writeVInt(0);
            return;
        }

        // NOTE: Convert sortable-longs to raw IEEE-754 bits so that XOR
        // produces small deltas for consecutive similar doubles.
        for (int i = 0; i < valueCount; i++) {
            values[i] = NumericUtils.sortableDoubleBits(values[i]);
        }

        out.writeVInt(valueCount);

        final byte[] scratch = scratchpad;
        initBitBuffer(scratch);

        writeBits(values[0], 64, out, scratch);

        long prevValue = values[0];

        // NOTE: Initialize to invalid window to force '11' (new window) case on first XOR.
        int prevLeadingZeros = 65;
        int prevMeaningfulBits = 0;

        for (int i = 1; i < valueCount; i++) {
            long bits = values[i] ^ prevValue;

            if (bits == 0) {
                writeBits(0, 1, out, scratch);
            } else {
                int leadingZeros = Long.numberOfLeadingZeros(bits);
                int trailingZeros = Long.numberOfTrailingZeros(bits);
                int meaningfulBits = 64 - leadingZeros - trailingZeros;

                int prevTrailingZeros = 64 - prevLeadingZeros - prevMeaningfulBits;

                if (leadingZeros >= prevLeadingZeros && trailingZeros >= prevTrailingZeros && prevMeaningfulBits > 0) {
                    writeBits(0b10, 2, out, scratch);
                    long windowBits = (bits >>> prevTrailingZeros) & mask(prevMeaningfulBits);
                    writeBits(windowBits, prevMeaningfulBits, out, scratch);
                } else {
                    writeBits(0b11, 2, out, scratch);

                    // NOTE: With 6-bit leading-zeros field, no clamping is needed.
                    // Store exact leadingZeros (0-63) directly.
                    writeBits(leadingZeros, LEADING_ZEROS_BITS, out, scratch);
                    writeBits(meaningfulBits - 1, MEANINGFUL_BITS_BITS, out, scratch);
                    writeBits(bits >>> trailingZeros, meaningfulBits, out, scratch);

                    prevLeadingZeros = leadingZeros;
                    prevMeaningfulBits = meaningfulBits;
                }
            }
            prevValue = values[i];
        }

        flushBits(out, scratch);
    }

    private static void initBitBuffer(byte[] scratch) {
        putLong(scratch, BIT_BUFFER_OFFSET, 0L);
        putInt(scratch, BITS_IN_BUFFER_OFFSET, 0);
    }

    private static void writeBits(long value, int numBits, DataOutput out, byte[] scratch) throws IOException {
        long buffer = getLong(scratch, BIT_BUFFER_OFFSET);
        int bitsInBuffer = getInt(scratch, BITS_IN_BUFFER_OFFSET);

        while (numBits > 0) {
            int available = 64 - bitsInBuffer;
            int toWrite = Math.min(numBits, available);

            long bits;
            if (toWrite == 64) {
                bits = value;
            } else {
                bits = (value >>> (numBits - toWrite)) & ((1L << toWrite) - 1);
            }

            if (toWrite >= 64) {
                buffer = bits;
            } else {
                buffer = (buffer << toWrite) | bits;
            }
            bitsInBuffer += toWrite;
            numBits -= toWrite;

            while (bitsInBuffer >= 8) {
                bitsInBuffer -= 8;
                out.writeByte((byte) ((buffer >>> bitsInBuffer) & 0xFF));
            }
            buffer &= mask(bitsInBuffer);
        }

        putLong(scratch, BIT_BUFFER_OFFSET, buffer);
        putInt(scratch, BITS_IN_BUFFER_OFFSET, bitsInBuffer);
    }

    private static void flushBits(DataOutput out, byte[] scratch) throws IOException {
        long buffer = getLong(scratch, BIT_BUFFER_OFFSET);
        int bitsInBuffer = getInt(scratch, BITS_IN_BUFFER_OFFSET);

        if (bitsInBuffer > 0) {
            int byteVal = (int) ((buffer << (8 - bitsInBuffer)) & 0xFF);
            out.writeByte((byte) byteVal);
        }
    }

    private static long mask(int numBits) {
        return numBits >= 64 ? -1L : (1L << numBits) - 1;
    }

    private static long getLong(byte[] b, int off) {
        return ((long) b[off] & 0xFF) << 56 | ((long) b[off + 1] & 0xFF) << 48 | ((long) b[off + 2] & 0xFF) << 40 | ((long) b[off + 3]
            & 0xFF) << 32 | ((long) b[off + 4] & 0xFF) << 24 | ((long) b[off + 5] & 0xFF) << 16 | ((long) b[off + 6] & 0xFF) << 8
            | ((long) b[off + 7] & 0xFF);
    }

    private static void putLong(byte[] b, int off, long val) {
        b[off] = (byte) (val >>> 56);
        b[off + 1] = (byte) (val >>> 48);
        b[off + 2] = (byte) (val >>> 40);
        b[off + 3] = (byte) (val >>> 32);
        b[off + 4] = (byte) (val >>> 24);
        b[off + 5] = (byte) (val >>> 16);
        b[off + 6] = (byte) (val >>> 8);
        b[off + 7] = (byte) val;
    }

    private static int getInt(byte[] b, int off) {
        return (b[off] & 0xFF) << 24 | (b[off + 1] & 0xFF) << 16 | (b[off + 2] & 0xFF) << 8 | (b[off + 3] & 0xFF);
    }

    private static void putInt(byte[] b, int off, int val) {
        b[off] = (byte) (val >>> 24);
        b[off + 1] = (byte) (val >>> 16);
        b[off + 2] = (byte) (val >>> 8);
        b[off + 3] = (byte) val;
    }

    @Override
    public boolean requiresExplicitClose() {
        return false;
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "GorillaEncodeStage";
    }
}
