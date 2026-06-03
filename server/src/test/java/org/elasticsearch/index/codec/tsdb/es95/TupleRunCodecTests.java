/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

/**
 * Round-trip and sizing tests for {@link TupleRunCodec}. Covers the dominant TSDB shapes:
 *
 * <ul>
 *   <li>Pure mid-tsid block: one tuple-run filling the whole block.</li>
 *   <li>Single-boundary block: two tuple-runs across a {@code _tsid} transition.</li>
 *   <li>Varying-K block: tuple-runs whose K differs across the boundary.</li>
 *   <li>Head-partial / tail-partial blocks: docs that straddle into neighboring blocks.</li>
 *   <li>Final partial block: fewer than blockSize valid ords, rest is decoder-padded zeros.</li>
 *   <li>Corrupt payloads: invalid period/runLen/K throw {@code CorruptIndexException}.</li>
 * </ul>
 */
public class TupleRunCodecTests extends ESTestCase {

    public void testRoundTripPureMidTsidK3() throws Exception {
        long[] tuple = { 10L, 20L, 30L };
        int K = tuple.length;
        long[] ords = new long[128];
        int[] perDocK = new int[129];
        int numDocs = 0;
        int pos = 0;
        for (int d = 0; d < 42; d++) {
            perDocK[numDocs++] = K;
            for (int k = 0; k < K; k++) {
                ords[pos++] = tuple[k];
            }
        }
        perDocK[numDocs++] = K;
        ords[pos++] = tuple[0];
        ords[pos++] = tuple[1];
        int tailMissing = 1;
        assertEquals(128, pos);

        TupleRunCodec.RunBuilder runs = new TupleRunCodec.RunBuilder(numDocs);
        TupleRunCodec.INSTANCE.buildRuns(ords, perDocK, numDocs, 0, tailMissing, runs);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        TupleRunCodec.INSTANCE.encodePayload(runs, 0, tailMissing, out);

        long estimate = TupleRunCodec.INSTANCE.estimateSize(runs, 0, tailMissing);
        assertEquals(estimate, out.size());

        ByteBuffersDataInput in = new ByteBuffersDataInput(out.toBufferList());
        long header = in.readVLong();
        byte subMode = in.readByte();
        assertEquals(TupleRunCodec.ENCODING, Long.numberOfTrailingZeros(~header));
        assertEquals(TupleRunCodec.SUB_MODE, subMode);
        long[] decoded = new long[128];
        TupleRunCodec.INSTANCE.decodePayload(in, decoded);
        assertArrayEquals(ords, decoded);
    }

    public void testRoundTripSingleBoundaryBlock() throws Exception {
        long[] tupleA = { 100L, 200L, 300L };
        long[] tupleB = { 400L, 500L, 600L };
        long[] ords = new long[128];
        int[] perDocK = new int[129];
        int numDocs = 0;
        int pos = 0;
        for (int d = 0; d < 20; d++) {
            perDocK[numDocs++] = 3;
            for (long o : tupleA) {
                ords[pos++] = o;
            }
        }
        for (int d = 0; d < 22; d++) {
            perDocK[numDocs++] = 3;
            for (long o : tupleB) {
                ords[pos++] = o;
            }
        }
        perDocK[numDocs++] = 3;
        ords[pos++] = tupleB[0];
        ords[pos++] = tupleB[1];
        int tailMissing = 1;
        assertEquals(128, pos);

        TupleRunCodec.RunBuilder runs = new TupleRunCodec.RunBuilder(numDocs);
        TupleRunCodec.INSTANCE.buildRuns(ords, perDocK, numDocs, 0, tailMissing, runs);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        TupleRunCodec.INSTANCE.encodePayload(runs, 0, tailMissing, out);

        long estimate = TupleRunCodec.INSTANCE.estimateSize(runs, 0, tailMissing);
        assertEquals(estimate, out.size());

        ByteBuffersDataInput in = new ByteBuffersDataInput(out.toBufferList());
        in.readVLong();
        in.readByte();
        long[] decoded = new long[128];
        TupleRunCodec.INSTANCE.decodePayload(in, decoded);
        assertArrayEquals(ords, decoded);
    }

    public void testRoundTripVaryingKAcrossBoundary() throws Exception {
        long[] tupleA = { 1L, 5L, 9L, 13L };
        long[] tupleB = { 7L, 17L };
        long[] ords = new long[128];
        int[] perDocK = new int[129];
        int numDocs = 0;
        int pos = 0;
        for (int d = 0; d < 16; d++) {
            perDocK[numDocs++] = 4;
            for (long o : tupleA) {
                ords[pos++] = o;
            }
        }
        for (int d = 0; d < 32; d++) {
            perDocK[numDocs++] = 2;
            for (long o : tupleB) {
                ords[pos++] = o;
            }
        }
        assertEquals(128, pos);

        TupleRunCodec.RunBuilder runs = new TupleRunCodec.RunBuilder(numDocs);
        TupleRunCodec.INSTANCE.buildRuns(ords, perDocK, numDocs, 0, 0, runs);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        TupleRunCodec.INSTANCE.encodePayload(runs, 0, 0, out);

        long estimate = TupleRunCodec.INSTANCE.estimateSize(runs, 0, 0);
        assertEquals(estimate, out.size());

        ByteBuffersDataInput in = new ByteBuffersDataInput(out.toBufferList());
        in.readVLong();
        in.readByte();
        long[] decoded = new long[128];
        TupleRunCodec.INSTANCE.decodePayload(in, decoded);
        assertArrayEquals(ords, decoded);
    }

    public void testRoundTripWithHeadPartial() throws Exception {
        // NOTE: first doc has K=4 with headOffset=2; only positions [2,3] sit in this block.
        long[] tuple = { 11L, 22L, 33L, 44L };
        int K = tuple.length;
        long[] ords = new long[128];
        int[] perDocK = new int[129];
        int numDocs = 0;
        int pos = 0;
        perDocK[numDocs++] = K;
        ords[pos++] = tuple[2];
        ords[pos++] = tuple[3];
        for (int d = 0; d < 31; d++) {
            perDocK[numDocs++] = K;
            for (long o : tuple) {
                ords[pos++] = o;
            }
        }
        perDocK[numDocs++] = K;
        ords[pos++] = tuple[0];
        ords[pos++] = tuple[1];
        int tailMissing = 2;
        int headOffset = 2;
        assertEquals(128, pos);

        TupleRunCodec.RunBuilder runs = new TupleRunCodec.RunBuilder(numDocs);
        TupleRunCodec.INSTANCE.buildRuns(ords, perDocK, numDocs, headOffset, tailMissing, runs);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        TupleRunCodec.INSTANCE.encodePayload(runs, headOffset, tailMissing, out);

        long estimate = TupleRunCodec.INSTANCE.estimateSize(runs, headOffset, tailMissing);
        assertEquals(estimate, out.size());

        ByteBuffersDataInput in = new ByteBuffersDataInput(out.toBufferList());
        in.readVLong();
        in.readByte();
        long[] decoded = new long[128];
        TupleRunCodec.INSTANCE.decodePayload(in, decoded);
        assertArrayEquals(ords, decoded);
    }

    public void testRoundTripFinalPartialBlock() throws Exception {
        // NOTE: 10 docs of K=5 fill only 50 of the 128 slots; the rest are zero-padded
        // by the writer and must round-trip unchanged through the decoder.
        long[] tuple = { 1L, 2L, 3L, 4L, 5L };
        int K = tuple.length;
        long[] ords = new long[128];
        int[] perDocK = new int[129];
        int numDocs = 0;
        int pos = 0;
        for (int d = 0; d < 10; d++) {
            perDocK[numDocs++] = K;
            for (long o : tuple) {
                ords[pos++] = o;
            }
        }

        TupleRunCodec.RunBuilder runs = new TupleRunCodec.RunBuilder(numDocs);
        TupleRunCodec.INSTANCE.buildRuns(ords, perDocK, numDocs, 0, 0, runs);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        TupleRunCodec.INSTANCE.encodePayload(runs, 0, 0, out);

        long estimate = TupleRunCodec.INSTANCE.estimateSize(runs, 0, 0);
        assertEquals(estimate, out.size());

        ByteBuffersDataInput in = new ByteBuffersDataInput(out.toBufferList());
        in.readVLong();
        in.readByte();
        long[] decoded = new long[128];
        TupleRunCodec.INSTANCE.decodePayload(in, decoded);
        long[] expected = new long[128];
        System.arraycopy(ords, 0, expected, 0, 50);
        assertArrayEquals(expected, decoded);
    }

    public void testEstimateIsSentinelForEmptyBlock() {
        long[] ords = new long[128];
        int[] perDocK = new int[0];
        TupleRunCodec.RunBuilder runs = new TupleRunCodec.RunBuilder(1);
        TupleRunCodec.INSTANCE.buildRuns(ords, perDocK, 0, 0, 0, runs);
        assertEquals(Long.MAX_VALUE, TupleRunCodec.INSTANCE.estimateSize(runs, 0, 0));
    }

    public void testCorruptRunLenThrows() throws Exception {
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        out.writeVInt(0);
        out.writeVInt(0);
        out.writeVInt(1);
        out.writeVInt(3);
        out.writeVInt(0);
        out.writeVLong(1L);
        out.writeVLong(0L);
        out.writeVLong(0L);
        long[] decoded = new long[128];
        expectThrows(
            CorruptIndexException.class,
            () -> TupleRunCodec.INSTANCE.decodePayload(new ByteBuffersDataInput(out.toBufferList()), decoded)
        );
    }

    public void testCorruptKThrows() throws Exception {
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        out.writeVInt(0);
        out.writeVInt(0);
        out.writeVInt(1);
        out.writeVInt(0);
        out.writeVInt(1);
        out.writeVLong(0L);
        long[] decoded = new long[128];
        expectThrows(
            CorruptIndexException.class,
            () -> TupleRunCodec.INSTANCE.decodePayload(new ByteBuffersDataInput(out.toBufferList()), decoded)
        );
    }

    public void testCorruptOverflowingTupleThrows() throws Exception {
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        out.writeVInt(0);
        out.writeVInt(0);
        out.writeVInt(1);
        out.writeVInt(2);
        out.writeVInt(200);
        out.writeVLong(0L);
        out.writeVLong(0L);
        long[] decoded = new long[128];
        expectThrows(
            CorruptIndexException.class,
            () -> TupleRunCodec.INSTANCE.decodePayload(new ByteBuffersDataInput(out.toBufferList()), decoded)
        );
    }

    public void testCyclicEmissionWithBothHeadAndTailPartialOnSingleRun() throws Exception {
        // NOTE: one logical run that has BOTH headOffset and tailMissing, exercising the
        // mid-cycle emission path that other tests do not hit.
        long[] tuple = { 7L, 14L, 21L, 28L };
        int K = tuple.length;
        int headOffset = 1;
        int tailMissing = 2;
        int runLen = 32;
        int totalOrds = runLen * K - headOffset - tailMissing;
        assertEquals(125, totalOrds);
        long[] expected = new long[128];
        int cursor = headOffset;
        for (int i = 0; i < totalOrds; i++) {
            expected[i] = tuple[cursor];
            cursor++;
            if (cursor == K) cursor = 0;
        }

        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        out.writeVInt(headOffset);
        out.writeVInt(tailMissing);
        out.writeVInt(1);
        out.writeVInt(K);
        out.writeVInt(runLen);
        out.writeVLong(tuple[0]);
        for (int k = 1; k < K; k++) {
            out.writeVLong(tuple[k] - tuple[k - 1] - 1);
        }

        long[] decoded = new long[128];
        TupleRunCodec.INSTANCE.decodePayload(new ByteBuffersDataInput(out.toBufferList()), decoded);
        Arrays.fill(expected, totalOrds, 128, 0L);
        assertArrayEquals(expected, decoded);
    }
}
