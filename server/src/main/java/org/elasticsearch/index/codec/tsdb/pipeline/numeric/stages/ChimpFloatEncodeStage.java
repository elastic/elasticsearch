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

public final class ChimpFloatEncodeStage implements PayloadEncoder {

    private static final int BIT_STATE_SIZE = 16;

    // NOTE: Leading-zero buckets for 32-bit floats, proportionally scaled from
    // the 64-bit buckets in the Chimp paper.
    static final int[] LEADING_ZERO_BUCKETS = { 0, 4, 6, 8, 10, 12, 14, 16 };
    private static final int BUCKET_BITS = 3;
    private static final int MEANINGFUL_BITS_BITS = 5;

    private static final int BIT_BUFFER_OFFSET = 0;
    private static final int BITS_IN_BUFFER_OFFSET = 8;

    private final byte[] bitState = new byte[BIT_STATE_SIZE];

    @Override
    public byte id() {
        return StageId.CHIMP_FLOAT_PAYLOAD.id;
    }

    // NOTE: Same Chimp algorithm as ChimpDoubleEncodeStage but operating on 32-bit
    // float values. The float pipeline stores sortable ints in the low 32 bits
    // of long[]. We convert to raw IEEE 754 float bits before XOR.
    // Control prefixes: '0' = identical, '10' = reuse window, '11' = new window.
    // Leading zeros use 3-bit bucket encoding with float-scaled bucket values.
    @Override
    public void encode(final long[] values, int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
        if (valueCount == 0) {
            out.writeVInt(0);
            return;
        }

        for (int i = 0; i < valueCount; i++) {
            values[i] = NumericUtils.sortableFloatBits((int) values[i]);
        }

        out.writeVInt(valueCount);

        final byte[] bits = bitState;
        initBitBuffer(bits);

        writeBits(values[0] & 0xFFFFFFFFL, 32, out, bits);

        int prevValue = (int) values[0];
        int prevLeadingZeros = 33;
        int prevMeaningfulBits = 0;

        for (int i = 1; i < valueCount; i++) {
            final int current = (int) values[i];
            final int xor = current ^ prevValue;

            if (xor == 0) {
                writeBits(0, 1, out, bits);
            } else {
                final int leadingZeros = Integer.numberOfLeadingZeros(xor);
                final int trailingZeros = Integer.numberOfTrailingZeros(xor);

                final int prevTrailingZeros = 32 - prevLeadingZeros - prevMeaningfulBits;

                if (leadingZeros >= prevLeadingZeros && trailingZeros >= prevTrailingZeros && prevMeaningfulBits > 0) {
                    writeBits(0b10, 2, out, bits);
                    long windowBits = ((long) (xor >>> prevTrailingZeros)) & mask(prevMeaningfulBits);
                    writeBits(windowBits, prevMeaningfulBits, out, bits);
                } else {
                    writeBits(0b11, 2, out, bits);

                    int bucketIndex = findBucketIndex(leadingZeros);
                    int roundedLeadingZeros = LEADING_ZERO_BUCKETS[bucketIndex];
                    int meaningfulBits = 32 - roundedLeadingZeros - trailingZeros;

                    writeBits(bucketIndex, BUCKET_BITS, out, bits);
                    writeBits(meaningfulBits - 1, MEANINGFUL_BITS_BITS, out, bits);
                    writeBits(xor >>> trailingZeros, meaningfulBits, out, bits);

                    prevLeadingZeros = roundedLeadingZeros;
                    prevMeaningfulBits = meaningfulBits;
                }
            }
            prevValue = current;
        }

        flushBits(out, bits);
    }

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
        return "ChimpFloatEncodeStage";
    }
}
