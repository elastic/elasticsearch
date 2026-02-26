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

public final class ChimpDoubleEncodeStage implements PayloadEncoder {

    private static final int BIT_STATE_SIZE = 16;

    // NOTE: Chimp's leading-zero bucket encoding. Instead of Gorilla's 6-bit direct
    // encoding, Chimp rounds down the actual leading-zero count to the nearest bucket
    // value and stores a 3-bit index. This saves 3 bits on the leading-zeros field at
    // the cost of encoding a few extra meaningful bits (from rounding down).
    static final int[] LEADING_ZERO_BUCKETS = { 0, 8, 12, 16, 18, 20, 22, 24 };
    private static final int BUCKET_BITS = 3;
    private static final int MEANINGFUL_BITS_BITS = 6;

    private static final int BIT_BUFFER_OFFSET = 0;
    private static final int BITS_IN_BUFFER_OFFSET = 8;

    private final byte[] bitState = new byte[BIT_STATE_SIZE];

    @Override
    public byte id() {
        return StageId.CHIMP_PAYLOAD.id;
    }

    // NOTE: Payload layout: [valueCount: VInt] [first value: 64 raw bits]
    // followed by a Chimp-encoded XOR bitstream. Each subsequent value is XORed
    // with its predecessor and encoded as one of three cases:
    // '0' — XOR is zero (value unchanged).
    // '10' — reuse previous window (prevLeadingZeros, prevMeaningfulBits).
    // '11' — new window: [leadingZeros: 3-bit bucket index]
    // [meaningfulBits-1: 6 bits] [meaningful bits].
    // Leading zeros are rounded down to the nearest bucket value from
    // {0, 8, 12, 16, 18, 20, 22, 24}, reducing the field from 6 to 3 bits.
    @Override
    public void encode(final long[] values, int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
        if (valueCount == 0) {
            out.writeVInt(0);
            return;
        }

        for (int i = 0; i < valueCount; i++) {
            values[i] = NumericUtils.sortableDoubleBits(values[i]);
        }

        out.writeVInt(valueCount);

        final byte[] bits = bitState;
        initBitBuffer(bits);

        writeBits(values[0], 64, out, bits);

        long prevValue = values[0];
        int prevLeadingZeros = 65;
        int prevMeaningfulBits = 0;

        for (int i = 1; i < valueCount; i++) {
            long xor = values[i] ^ prevValue;

            if (xor == 0) {
                writeBits(0, 1, out, bits);
            } else {
                int leadingZeros = Long.numberOfLeadingZeros(xor);
                int trailingZeros = Long.numberOfTrailingZeros(xor);

                int prevTrailingZeros = 64 - prevLeadingZeros - prevMeaningfulBits;

                if (leadingZeros >= prevLeadingZeros && trailingZeros >= prevTrailingZeros && prevMeaningfulBits > 0) {
                    writeBits(0b10, 2, out, bits);
                    long windowBits = (xor >>> prevTrailingZeros) & mask(prevMeaningfulBits);
                    writeBits(windowBits, prevMeaningfulBits, out, bits);
                } else {
                    writeBits(0b11, 2, out, bits);

                    int bucketIndex = findBucketIndex(leadingZeros);
                    int roundedLeadingZeros = LEADING_ZERO_BUCKETS[bucketIndex];
                    int meaningfulBits = 64 - roundedLeadingZeros - trailingZeros;

                    writeBits(bucketIndex, BUCKET_BITS, out, bits);
                    writeBits(meaningfulBits - 1, MEANINGFUL_BITS_BITS, out, bits);
                    writeBits(xor >>> trailingZeros, meaningfulBits, out, bits);

                    prevLeadingZeros = roundedLeadingZeros;
                    prevMeaningfulBits = meaningfulBits;
                }
            }
            prevValue = values[i];
        }

        flushBits(out, bits);
    }

    // NOTE: Finds the largest bucket index whose value is <= leadingZeros.
    static int findBucketIndex(int leadingZeros) {
        for (int i = LEADING_ZERO_BUCKETS.length - 1; i > 0; i--) {
            if (LEADING_ZERO_BUCKETS[i] <= leadingZeros) {
                return i;
            }
        }
        return 0;
    }

    private static void initBitBuffer(byte[] bits) {
        putLong(bits, BIT_BUFFER_OFFSET, 0L);
        putInt(bits, BITS_IN_BUFFER_OFFSET, 0);
    }

    private static void writeBits(long value, int numBits, DataOutput out, byte[] bits) throws IOException {
        long buffer = getLong(bits, BIT_BUFFER_OFFSET);
        int bitsInBuffer = getInt(bits, BITS_IN_BUFFER_OFFSET);

        while (numBits > 0) {
            int available = 64 - bitsInBuffer;
            int toWrite = Math.min(numBits, available);

            long chunk;
            if (toWrite == 64) {
                chunk = value;
            } else {
                chunk = (value >>> (numBits - toWrite)) & ((1L << toWrite) - 1);
            }

            if (toWrite >= 64) {
                buffer = chunk;
            } else {
                buffer = (buffer << toWrite) | chunk;
            }
            bitsInBuffer += toWrite;
            numBits -= toWrite;

            while (bitsInBuffer >= 8) {
                bitsInBuffer -= 8;
                out.writeByte((byte) ((buffer >>> bitsInBuffer) & 0xFF));
            }
            buffer &= mask(bitsInBuffer);
        }

        putLong(bits, BIT_BUFFER_OFFSET, buffer);
        putInt(bits, BITS_IN_BUFFER_OFFSET, bitsInBuffer);
    }

    private static void flushBits(DataOutput out, byte[] bits) throws IOException {
        long buffer = getLong(bits, BIT_BUFFER_OFFSET);
        int bitsInBuffer = getInt(bits, BITS_IN_BUFFER_OFFSET);

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
    public String toString() {
        return "ChimpDoubleEncodeStage";
    }
}
