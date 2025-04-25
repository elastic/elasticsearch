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
package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocBaseBitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.hnsw.IntToIntFunction;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class is used to write and read the doc ids in a compressed format. The format is optimized
 * for the number of bits per value (bpv) and the number of values.
 *
 * <p>It is copied from the BKD implementation.
 */
final class DocIdsWriter {
    public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 512;

    private static final byte CONTINUOUS_IDS = (byte) -2;
    private static final byte BITSET_IDS = (byte) -1;
    private static final byte DELTA_BPV_16 = (byte) 16;
    private static final byte BPV_21 = (byte) 21;
    private static final byte BPV_24 = (byte) 24;
    private static final byte BPV_32 = (byte) 32;

    private int[] scratch = new int[0];
    private final LongsRef scratchLongs = new LongsRef();

    /**
     * IntsRef to be used to iterate over the scratch buffer. A single instance is reused to avoid
     * re-allocating the object. The ints and length fields need to be reset each use.
     *
     * <p>The main reason for existing is to be able to call the {@link
     * IntersectVisitor#visit(IntsRef)} method rather than the {@link IntersectVisitor#visit(int)}
     * method. This seems to make a difference in performance, probably due to fewer virtual calls
     * then happening (once per read call rather than once per doc).
     */
    private final IntsRef scratchIntsRef = new IntsRef();

    {
        // This is here to not rely on the default constructor of IntsRef to set offset to 0
        scratchIntsRef.offset = 0;
    }

    DocIdsWriter() {}

    void writeDocIds(IntToIntFunction docIds, int count, DataOutput out) throws IOException {
        // docs can be sorted either when all docs in a block have the same value
        // or when a segment is sorted
        if (count == 0) {
            out.writeByte(CONTINUOUS_IDS);
            return;
        }
        if (count > scratch.length) {
            scratch = new int[count];
        }
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
        if (strictlySorted) {
            if (min2max == count) {
                // continuous ids, typically happens when segment is sorted
                out.writeByte(CONTINUOUS_IDS);
                out.writeVInt(docIds.apply(0));
                return;
            } else if (min2max <= (count << 4)) {
                assert min2max > count : "min2max: " + min2max + ", count: " + count;
                // Only trigger bitset optimization when max - min + 1 <= 16 * count in order to avoid
                // expanding too much storage.
                // A field with lower cardinality will have higher probability to trigger this optimization.
                out.writeByte(BITSET_IDS);
                writeIdsAsBitSet(docIds, count, out);
                return;
            }
        }

        if (min2max <= 0xFFFF) {
            out.writeByte(DELTA_BPV_16);
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
        } else {
            if (max <= 0x1FFFFF) {
                out.writeByte(BPV_21);
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
            } else if (max <= 0xFFFFFF) {
                out.writeByte(BPV_24);

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
            } else {
                out.writeByte(BPV_32);
                for (int i = 0; i < count; i++) {
                    out.writeInt(docIds.apply(i));
                }
            }
        }
    }

    private static void writeIdsAsBitSet(IntToIntFunction docIds, int count, DataOutput out) throws IOException {
        int min = docIds.apply(0);
        int max = docIds.apply(count - 1);

        final int offsetWords = min >> 6;
        final int offsetBits = offsetWords << 6;
        final int totalWordCount = FixedBitSet.bits2words(max - offsetBits + 1);
        long currentWord = 0;
        int currentWordIndex = 0;

        out.writeVInt(offsetWords);
        out.writeVInt(totalWordCount);
        // build bit set streaming
        for (int i = 0; i < count; i++) {
            final int index = docIds.apply(i) - offsetBits;
            final int nextWordIndex = index >> 6;
            assert currentWordIndex <= nextWordIndex;
            if (currentWordIndex < nextWordIndex) {
                out.writeLong(currentWord);
                currentWord = 0L;
                currentWordIndex++;
                while (currentWordIndex < nextWordIndex) {
                    currentWordIndex++;
                    out.writeLong(0L);
                }
            }
            currentWord |= 1L << index;
        }
        out.writeLong(currentWord);
        assert currentWordIndex + 1 == totalWordCount;
    }

    /** Read {@code count} integers into {@code docIDs}. */
    void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
        if (count > scratch.length) {
            scratch = new int[count];
        }
        final int bpv = in.readByte();
        switch (bpv) {
            case CONTINUOUS_IDS:
                readContinuousIds(in, count, docIDs);
                break;
            case BITSET_IDS:
                readBitSet(in, count, docIDs);
                break;
            case DELTA_BPV_16:
                readDelta16(in, count, docIDs);
                break;
            case BPV_21:
                readInts21(in, count, docIDs);
                break;
            case BPV_24:
                readInts24(in, count, docIDs);
                break;
            case BPV_32:
                readInts32(in, count, docIDs);
                break;
            default:
                throw new IOException("Unsupported number of bits per value: " + bpv);
        }
    }

    private DocIdSetIterator readBitSetIterator(IndexInput in, int count) throws IOException {
        int offsetWords = in.readVInt();
        int longLen = in.readVInt();
        scratchLongs.longs = ArrayUtil.growNoCopy(scratchLongs.longs, longLen);
        in.readLongs(scratchLongs.longs, 0, longLen);
        // make ghost bits clear for FixedBitSet.
        if (longLen < scratchLongs.length) {
            Arrays.fill(scratchLongs.longs, longLen, scratchLongs.longs.length, 0);
        }
        scratchLongs.length = longLen;
        FixedBitSet bitSet = new FixedBitSet(scratchLongs.longs, longLen << 6);
        return new DocBaseBitSetIterator(bitSet, count, offsetWords << 6);
    }

    private static void readContinuousIds(IndexInput in, int count, int[] docIDs) throws IOException {
        int start = in.readVInt();
        for (int i = 0; i < count; i++) {
            docIDs[i] = start + i;
        }
    }

    private void readBitSet(IndexInput in, int count, int[] docIDs) throws IOException {
        DocIdSetIterator iterator = readBitSetIterator(in, count);
        int docId, pos = 0;
        while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            docIDs[pos++] = docId;
        }
        assert pos == count : "pos: " + pos + ", count: " + count;
    }

    private static void readDelta16(IndexInput in, int count, int[] docIds) throws IOException {
        final int min = in.readVInt();
        final int half = count >> 1;
        in.readInts(docIds, 0, half);
        if (count == DEFAULT_MAX_POINTS_IN_LEAF_NODE) {
            // Same format, but enabling the JVM to specialize the decoding logic for the default number
            // of points per node proved to help on benchmarks
            decode16(docIds, DEFAULT_MAX_POINTS_IN_LEAF_NODE / 2, min);
        } else {
            decode16(docIds, half, min);
        }
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
        if (count == DEFAULT_MAX_POINTS_IN_LEAF_NODE) {
            // Same format, but enabling the JVM to specialize the decoding logic for the default number
            // of points per node proved to help on benchmarks
            decode21(
                docIDs,
                scratch,
                floorToMultipleOf16(DEFAULT_MAX_POINTS_IN_LEAF_NODE / 3),
                floorToMultipleOf16(DEFAULT_MAX_POINTS_IN_LEAF_NODE / 3) * 2
            );
        } else {
            decode21(docIDs, scratch, oneThird, numInts);
        }
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
        if (count == DEFAULT_MAX_POINTS_IN_LEAF_NODE) {
            // Same format, but enabling the JVM to specialize the decoding logic for the default number
            // of points per node proved to help on benchmarks
            assert floorToMultipleOf16(quarter) == quarter
                : "We are relying on the fact that quarter of DEFAULT_MAX_POINTS_IN_LEAF_NODE"
                    + " is a multiple of 16 to vectorize the decoding loop,"
                    + " please check performance issue if you want to break this assumption.";
            decode24(docIDs, scratch, DEFAULT_MAX_POINTS_IN_LEAF_NODE / 4, DEFAULT_MAX_POINTS_IN_LEAF_NODE / 4 * 3);
        } else {
            decode24(docIDs, scratch, quarter, numInts);
        }
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
