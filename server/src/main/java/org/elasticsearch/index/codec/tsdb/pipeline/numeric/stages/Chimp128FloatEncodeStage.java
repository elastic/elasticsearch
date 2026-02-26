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

public final class Chimp128FloatEncodeStage implements PayloadEncoder {

    private static final int BIT_STATE_SIZE = 16;
    private static final int BUCKET_BITS = 3;
    private static final int MEANINGFUL_BITS_BITS = 5;
    private static final int BIT_BUFFER_OFFSET = 0;
    private static final int BITS_IN_BUFFER_OFFSET = 8;

    private final int bufferSize;
    private final int bufferMask;
    private final int indexBits;
    private final long[] ring;
    private final byte[] bitState = new byte[BIT_STATE_SIZE];

    public Chimp128FloatEncodeStage() {
        this(128);
    }

    public Chimp128FloatEncodeStage(final int bufferSize) {
        assert bufferSize > 0 && (bufferSize & (bufferSize - 1)) == 0 : "bufferSize must be a power of 2";
        this.bufferSize = bufferSize;
        this.bufferMask = bufferSize - 1;
        this.indexBits = bufferSize == 1 ? 0 : 32 - Integer.numberOfLeadingZeros(bufferSize - 1);
        this.ring = new long[bufferSize];
    }

    @Override
    public byte id() {
        return StageId.CHIMP128_FLOAT_PAYLOAD.id;
    }

    // NOTE: Payload layout: [valueCount: VInt] [bufferSizeLog2: VInt] [first value: 32 raw bits]
    // followed by a Chimp128-encoded XOR bitstream for 32-bit floats. Each subsequent value is
    // XORed against the best match in a ring buffer of previous values (the one maximizing trailing
    // zeros). Each entry is encoded as one of three cases:
    // '0' [index: indexBits] — XOR is zero (exact match in ring buffer).
    // '10' [index: indexBits] [meaningful bits in previous window] — reuse previous window.
    // '11' [index: indexBits] [bucket: 3 bits] [meaningfulBits-1: 5 bits] [meaningful bits] — new window.
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
        out.writeVInt(Integer.numberOfTrailingZeros(bufferSize));

        final byte[] bits = bitState;
        initBitBuffer(bits);

        writeBits(values[0] & 0xFFFFFFFFL, 32, out, bits);

        int head = 0;
        ring[head] = values[0] & 0xFFFFFFFFL;
        int ringCount = 1;

        int prevLeadingZeros = 33;
        int prevMeaningfulBits = 0;

        for (int i = 1; i < valueCount; i++) {
            final int current = (int) values[i];

            // Scan ring buffer to find best reference (max trailing zeros)
            int bestIndex = 0;
            int bestTrailing = -1;
            for (int j = 0; j < ringCount; j++) {
                final int ref = (int) ring[(head - j) & bufferMask];
                final int xorCandidate = current ^ ref;
                final int trailing = xorCandidate == 0 ? 33 : Integer.numberOfTrailingZeros(xorCandidate);
                if (trailing > bestTrailing) {
                    bestTrailing = trailing;
                    bestIndex = j;
                }
            }

            final int ref = (int) ring[(head - bestIndex) & bufferMask];
            final int xor = current ^ ref;

            if (xor == 0) {
                writeBits(0, 1, out, bits);
                if (indexBits > 0) {
                    writeBits(bestIndex, indexBits, out, bits);
                }
            } else {
                final int leadingZeros = Integer.numberOfLeadingZeros(xor);
                final int trailingZeros = Integer.numberOfTrailingZeros(xor);
                final int prevTrailingZeros = 32 - prevLeadingZeros - prevMeaningfulBits;

                if (leadingZeros >= prevLeadingZeros && trailingZeros >= prevTrailingZeros && prevMeaningfulBits > 0) {
                    writeBits(0b10, 2, out, bits);
                    if (indexBits > 0) {
                        writeBits(bestIndex, indexBits, out, bits);
                    }
                    final long windowBits = ((long) (xor >>> prevTrailingZeros)) & mask(prevMeaningfulBits);
                    writeBits(windowBits, prevMeaningfulBits, out, bits);
                } else {
                    writeBits(0b11, 2, out, bits);
                    if (indexBits > 0) {
                        writeBits(bestIndex, indexBits, out, bits);
                    }

                    final int bucketIndex = ChimpFloatEncodeStage.findBucketIndex(leadingZeros);
                    final int roundedLeadingZeros = ChimpFloatEncodeStage.LEADING_ZERO_BUCKETS[bucketIndex];
                    final int meaningfulBits = 32 - roundedLeadingZeros - trailingZeros;

                    writeBits(bucketIndex, BUCKET_BITS, out, bits);
                    writeBits(meaningfulBits - 1, MEANINGFUL_BITS_BITS, out, bits);
                    writeBits(xor >>> trailingZeros, meaningfulBits, out, bits);

                    prevLeadingZeros = roundedLeadingZeros;
                    prevMeaningfulBits = meaningfulBits;
                }
            }

            head = (head + 1) & bufferMask;
            ring[head] = current & 0xFFFFFFFFL;
            if (ringCount < bufferSize) {
                ringCount++;
            }
        }

        flushBits(out, bits);
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
        return "Chimp128FloatEncodeStage";
    }
}
