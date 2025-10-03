/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.IntToIntFunction;

import java.io.IOException;

/**
 * This class is used to write and read the doc ids in a compressed format. The format is optimized
 * for the number of bits per value (bpv) and the number of values.
 *
 * <p>It is copied from the BKD implementation.
 */
public final class DocIdsWriter {

    private static final byte CONTINUOUS_IDS = (byte) -2;
    private static final byte DELTA_BPV_16 = (byte) 16;
    private static final byte BPV_21 = (byte) 21;
    private static final byte BPV_24 = (byte) 24;
    private static final byte BPV_32 = (byte) 32;

    private int[] scratch = new int[0];

    public DocIdsWriter() {}

    /**
     * Calculate the best encoding that will be used to write blocks of doc ids of blockSize.
     * The encoding choice is universal for all the blocks, which means that the encoding is only as
     * efficient as the worst block.
     * @param docIds function to access the doc ids
     * @param count number of doc ids
     * @param blockSize the block size
     * @return the byte encoding to use for the blocks
     */
    public byte calculateBlockEncoding(IntToIntFunction docIds, int count, int blockSize) {
        if (count == 0) {
            return CONTINUOUS_IDS;
        }
        int iterationLimit = count - blockSize + 1;
        int i = 0;
        int maxValue = 0;
        int maxMin2Max = 0;
        boolean continuousIds = true;
        for (; i < iterationLimit; i += blockSize) {
            int offset = i;
            var r = sortedAndMaxAndMin2Max(d -> docIds.apply(offset + d), blockSize);
            continuousIds &= r[0] == 1;
            maxValue = Math.max(maxValue, r[1]);
            maxMin2Max = Math.max(maxMin2Max, r[2]);
        }
        // check the tail
        if (i < count) {
            int offset = i;
            var r = sortedAndMaxAndMin2Max(d -> docIds.apply(offset + d), count - i);
            continuousIds &= r[0] == 1;
            maxValue = Math.max(maxValue, r[1]);
            maxMin2Max = Math.max(maxMin2Max, r[2]);
        }
        if (continuousIds) {
            return CONTINUOUS_IDS;
        } else if (maxMin2Max <= 0xFFFF) {
            return DELTA_BPV_16;
        } else {
            if (maxValue <= 0x1FFFFF) {
                return BPV_21;
            } else if (maxValue <= 0xFFFFFF) {
                return BPV_24;
            } else {
                return BPV_32;
            }
        }
    }

    public void writeDocIds(IntToIntFunction docIds, int count, byte encoding, DataOutput out) throws IOException {
        if (count == 0) {
            return;
        }
        if (count > scratch.length) {
            scratch = new int[count];
        }
        int min = docIds.apply(0);
        for (int i = 1; i < count; ++i) {
            int current = docIds.apply(i);
            min = Math.min(min, current);
        }
        switch (encoding) {
            case CONTINUOUS_IDS -> writeContinuousIds(docIds, count, out);
            case DELTA_BPV_16 -> writeDelta16(docIds, count, min, out);
            case BPV_21 -> write21(docIds, count, min, out);
            case BPV_24 -> write24(docIds, count, min, out);
            case BPV_32 -> write32(docIds, count, min, out);
            default -> throw new IOException("Unsupported number of bits per value: " + encoding);
        }
    }

    private static void writeContinuousIds(IntToIntFunction docIds, int count, DataOutput out) throws IOException {
        out.writeVInt(docIds.apply(0));
    }

    private void writeDelta16(IntToIntFunction docIds, int count, int min, DataOutput out) throws IOException {
        for (int i = 0; i < count; i++) {
            scratch[i] = docIds.apply(i) - min;
        }
        out.writeVInt(min);
        final int halfLen = count >> 1;
        for (int i = 0; i < halfLen; ++i) {
            scratch[i] = scratch[halfLen + i] | (scratch[i] << 16);
        }
        for (int i = 0; i < halfLen; i++) {
            out.writeInt(scratch[i]);
        }
        if ((count & 1) == 1) {
            out.writeShort((short) scratch[count - 1]);
        }
    }

    private void write21(IntToIntFunction docIds, int count, int min, DataOutput out) throws IOException {
        final int oneThird = floorToMultipleOf16(count / 3);
        final int numInts = oneThird * 2;
        for (int i = 0; i < numInts; i++) {
            scratch[i] = docIds.apply(i) << 11;
        }
        for (int i = 0; i < oneThird; i++) {
            final int longIdx = i + numInts;
            scratch[i] |= docIds.apply(longIdx) & 0x7FF;
            scratch[i + oneThird] |= (docIds.apply(longIdx) >>> 11) & 0x7FF;
        }
        for (int i = 0; i < numInts; i++) {
            out.writeInt(scratch[i]);
        }
        int i = oneThird * 3;
        for (; i < count - 2; i += 3) {
            out.writeLong(((long) docIds.apply(i)) | (((long) docIds.apply(i + 1)) << 21) | (((long) docIds.apply(i + 2)) << 42));
        }
        for (; i < count; ++i) {
            out.writeShort((short) docIds.apply(i));
            out.writeByte((byte) (docIds.apply(i) >>> 16));
        }
    }

    private void write24(IntToIntFunction docIds, int count, int min, DataOutput out) throws IOException {

        // encode the docs in the format that can be vectorized decoded.
        final int quarter = count >> 2;
        final int numInts = quarter * 3;
        for (int i = 0; i < numInts; i++) {
            scratch[i] = docIds.apply(i) << 8;
        }
        for (int i = 0; i < quarter; i++) {
            final int longIdx = i + numInts;
            scratch[i] |= docIds.apply(longIdx) & 0xFF;
            scratch[i + quarter] |= (docIds.apply(longIdx) >>> 8) & 0xFF;
            scratch[i + quarter * 2] |= docIds.apply(longIdx) >>> 16;
        }
        for (int i = 0; i < numInts; i++) {
            out.writeInt(scratch[i]);
        }
        for (int i = quarter << 2; i < count; ++i) {
            out.writeShort((short) docIds.apply(i));
            out.writeByte((byte) (docIds.apply(i) >>> 16));
        }
    }

    private void write32(IntToIntFunction docIds, int count, int min, DataOutput out) throws IOException {
        for (int i = 0; i < count; i++) {
            out.writeInt(docIds.apply(i));
        }
    }

    private static int[] sortedAndMaxAndMin2Max(IntToIntFunction docIds, int count) {
        // docs can be sorted either when all docs in a block have the same value
        // or when a segment is sorted
        boolean strictlySorted = true;
        int min = docIds.apply(0);
        int max = min;
        for (int i = 1; i < count; ++i) {
            int last = docIds.apply(i - 1);
            int current = docIds.apply(i);
            if (last >= current) {
                strictlySorted = false;
            }
            min = Math.min(min, current);
            max = Math.max(max, current);
        }

        int min2max = max - min + 1;
        return new int[] { (strictlySorted && min2max == count) ? 1 : 0, max, min2max };
    }

    public void writeDocIds(IntToIntFunction docIds, int count, DataOutput out) throws IOException {
        if (count == 0) {
            return;
        }
        if (count > scratch.length) {
            scratch = new int[count];
        }
        // docs can be sorted either when all docs in a block have the same value
        // or when a segment is sorted
        boolean strictlySorted = true;
        int min = docIds.apply(0);
        int max = min;
        for (int i = 1; i < count; ++i) {
            int last = docIds.apply(i - 1);
            int current = docIds.apply(i);
            if (last >= current) {
                strictlySorted = false;
            }
            min = Math.min(min, current);
            max = Math.max(max, current);
        }

        int min2max = max - min + 1;
        if (strictlySorted && min2max == count) {
            // continuous ids, typically happens when segment is sorted
            out.writeByte(CONTINUOUS_IDS);
            writeContinuousIds(docIds, count, out);
            return;
        }

        if (min2max <= 0xFFFF) {
            out.writeByte(DELTA_BPV_16);
            writeDelta16(docIds, count, min, out);
        } else {
            if (max <= 0x1FFFFF) {
                out.writeByte(BPV_21);
                write21(docIds, count, min, out);
            } else if (max <= 0xFFFFFF) {
                out.writeByte(BPV_24);
                write24(docIds, count, min, out);
            } else {
                out.writeByte(BPV_32);
                write32(docIds, count, min, out);
            }
        }
    }

    public void readInts(IndexInput in, int count, byte encoding, int[] docIDs) throws IOException {
        if (count == 0) {
            return;
        }
        if (count > scratch.length) {
            scratch = new int[count];
        }
        switch (encoding) {
            case CONTINUOUS_IDS -> readContinuousIds(in, count, docIDs);
            case DELTA_BPV_16 -> readDelta16(in, count, docIDs);
            case BPV_21 -> readInts21(in, count, docIDs);
            case BPV_24 -> readInts24(in, count, docIDs);
            case BPV_32 -> readInts32(in, count, docIDs);
            default -> throw new IOException("Unsupported number of bits per value: " + encoding);
        }
    }

    /** Read {@code count} integers into {@code docIDs}. */
    public void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
        if (count == 0) {
            return;
        }
        if (count > scratch.length) {
            scratch = new int[count];
        }
        final int bpv = in.readByte();
        readInts(in, count, (byte) bpv, docIDs);
    }

    private static void readContinuousIds(IndexInput in, int count, int[] docIDs) throws IOException {
        int start = in.readVInt();
        for (int i = 0; i < count; i++) {
            docIDs[i] = start + i;
        }
    }

    private static void readDelta16(IndexInput in, int count, int[] docIds) throws IOException {
        final int min = in.readVInt();
        final int half = count >> 1;
        in.readInts(docIds, 0, half);
        decode16(docIds, half, min);
        // read the remaining doc if count is odd.
        for (int i = half << 1; i < count; i++) {
            docIds[i] = Short.toUnsignedInt(in.readShort()) + min;
        }
    }

    private static void decode16(int[] docIDs, int half, int min) {
        for (int i = 0; i < half; ++i) {
            final int l = docIDs[i];
            docIDs[i] = (l >>> 16) + min;
            docIDs[i + half] = (l & 0xFFFF) + min;
        }
    }

    private static int floorToMultipleOf16(int n) {
        assert n >= 0;
        return n & 0xFFFFFFF0;
    }

    private void readInts21(IndexInput in, int count, int[] docIDs) throws IOException {
        int oneThird = floorToMultipleOf16(count / 3);
        int numInts = oneThird << 1;
        in.readInts(scratch, 0, numInts);
        decode21(docIDs, scratch, oneThird, numInts);
        int i = oneThird * 3;
        for (; i < count - 2; i += 3) {
            long l = in.readLong();
            docIDs[i] = (int) (l & 0x1FFFFFL);
            docIDs[i + 1] = (int) ((l >>> 21) & 0x1FFFFFL);
            docIDs[i + 2] = (int) (l >>> 42);
        }
        for (; i < count; ++i) {
            docIDs[i] = (in.readShort() & 0xFFFF) | (in.readByte() & 0xFF) << 16;
        }
    }

    private static void decode21(int[] docIds, int[] scratch, int oneThird, int numInts) {
        for (int i = 0; i < numInts; ++i) {
            docIds[i] = scratch[i] >>> 11;
        }
        for (int i = 0; i < oneThird; i++) {
            docIds[i + numInts] = (scratch[i] & 0x7FF) | ((scratch[i + oneThird] & 0x7FF) << 11);
        }
    }

    private void readInts24(IndexInput in, int count, int[] docIDs) throws IOException {
        int quarter = count >> 2;
        int numInts = quarter * 3;
        in.readInts(scratch, 0, numInts);
        decode24(docIDs, scratch, quarter, numInts);
        // Now read the remaining 0, 1, 2 or 3 values
        for (int i = quarter << 2; i < count; ++i) {
            docIDs[i] = (in.readShort() & 0xFFFF) | (in.readByte() & 0xFF) << 16;
        }
    }

    private static void decode24(int[] docIDs, int[] scratch, int quarter, int numInts) {
        for (int i = 0; i < numInts; ++i) {
            docIDs[i] = scratch[i] >>> 8;
        }
        for (int i = 0; i < quarter; i++) {
            docIDs[i + numInts] = (scratch[i] & 0xFF) | ((scratch[i + quarter] & 0xFF) << 8) | ((scratch[i + quarter * 2] & 0xFF) << 16);
        }
    }

    private static void readInts32(IndexInput in, int count, int[] docIDs) throws IOException {
        in.readInts(docIDs, 0, count);
    }
}
