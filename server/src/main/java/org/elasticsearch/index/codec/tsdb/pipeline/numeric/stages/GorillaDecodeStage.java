/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadDecoder;

import java.io.IOException;

public final class GorillaDecodeStage implements PayloadDecoder {

    private static final int BIT_STATE_SIZE = 16;
    private static final int LEADING_ZEROS_BITS = 6;
    private static final int MEANINGFUL_BITS_BITS = 6;
    private static final int BIT_BUFFER_OFFSET = 0;
    private static final int BITS_IN_BUFFER_OFFSET = 8;

    private final byte[] bitState = new byte[BIT_STATE_SIZE];

    @Override
    public byte id() {
        return StageId.GORILLA_PAYLOAD.id;
    }

    // NOTE: Payload layout: [valueCount: VInt] [first value: 64 raw bits]
    // followed by a Gorilla-encoded XOR bitstream. Each subsequent value is XORed
    // with its predecessor and encoded as one of three cases:
    // '0' — XOR is zero (value unchanged).
    // '10' — reuse previous window (prevLeadingZeros, prevMeaningfulBits).
    // '11' — new window: [leadingZeros: 6 bits] [meaningfulBits-1: 6 bits] [meaningful bits].
    // Values are stored as raw IEEE 754 bits; decoded back to sortable-longs at the end.
    @Override
    public int decode(final long[] values, final DataInput in, final DecodingContext context) throws IOException {
        int valueCount = in.readVInt();
        if (valueCount == 0) {
            return 0;
        }

        final byte[] bits = bitState;
        initBitBuffer(bits);

        values[0] = readBits(64, in, bits);

        long prevValue = values[0];
        int prevLeadingZeros = 65;
        int prevMeaningfulBits = 0;

        for (int i = 1; i < valueCount; i++) {
            final int firstBit = (int) readBits(1, in, bits);

            if (firstBit == 0) {
                values[i] = prevValue;
                continue;
            }

            final int secondBit = (int) readBits(1, in, bits);

            final long xor;
            if (secondBit == 0) {
                final int prevTrailingZeros = 64 - prevLeadingZeros - prevMeaningfulBits;
                final long windowBits = readBits(prevMeaningfulBits, in, bits);
                xor = windowBits << prevTrailingZeros;
            } else {
                final int leadingZeros = (int) readBits(LEADING_ZEROS_BITS, in, bits);
                final int meaningfulBits = (int) readBits(MEANINGFUL_BITS_BITS, in, bits) + 1;
                final int trailingZeros = 64 - leadingZeros - meaningfulBits;

                final long meaningful = readBits(meaningfulBits, in, bits);
                xor = meaningful << trailingZeros;

                prevLeadingZeros = leadingZeros;
                prevMeaningfulBits = meaningfulBits;
            }
            values[i] = prevValue ^ xor;
            prevValue = values[i];
        }

        for (int i = 0; i < valueCount; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Double.longBitsToDouble(values[i]));
        }

        return valueCount;
    }

    private static void initBitBuffer(byte[] bits) {
        putLong(bits, BIT_BUFFER_OFFSET, 0L);
        putInt(bits, BITS_IN_BUFFER_OFFSET, 0);
    }

    private static long readBits(int numBits, DataInput in, byte[] bits) throws IOException {
        if (numBits == 0) return 0;

        long buffer = getLong(bits, BIT_BUFFER_OFFSET);
        int bitsInBuffer = getInt(bits, BITS_IN_BUFFER_OFFSET);

        if (numBits > 56) {
            long result = 0;
            if (bitsInBuffer > 0) {
                result = buffer & mask(bitsInBuffer);
                numBits -= bitsInBuffer;
                bitsInBuffer = 0;
            }
            while (numBits >= 8) {
                int byteVal = in.readByte() & 0xFF;
                result = (result << 8) | byteVal;
                numBits -= 8;
            }
            if (numBits > 0) {
                int byteVal = in.readByte() & 0xFF;
                buffer = byteVal;
                bitsInBuffer = 8 - numBits;
                long chunk = (buffer >>> bitsInBuffer) & mask(numBits);
                result = (result << numBits) | chunk;
            }
            putLong(bits, BIT_BUFFER_OFFSET, buffer);
            putInt(bits, BITS_IN_BUFFER_OFFSET, bitsInBuffer);
            return result;
        }

        while (bitsInBuffer < numBits) {
            int byteVal = in.readByte() & 0xFF;
            buffer = (buffer << 8) | byteVal;
            bitsInBuffer += 8;
        }

        bitsInBuffer -= numBits;
        long result = (buffer >>> bitsInBuffer) & mask(numBits);
        buffer &= mask(bitsInBuffer);

        putLong(bits, BIT_BUFFER_OFFSET, buffer);
        putInt(bits, BITS_IN_BUFFER_OFFSET, bitsInBuffer);
        return result;
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
    public String toString() {
        return "GorillaDecodeStage";
    }
}
