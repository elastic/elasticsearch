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
        // NOTE: 42 docs of [10, 20, 30] = 126 ords, plus 2 leftover for partial-tail
        // contribution; here we fit exactly with 42 full docs + 2/3 of the next.
        long[] tuple = { 10L, 20L, 30L };
        int K = tuple.length;
        long[] ords = new long[128];
        int[] perDocK = new int[129];
        int numDocs = 0;
        int pos = 0;
        int tailMissing;
        // Fill 42 full docs of K=3 = 126 ords; one more partial doc fills 2 of K=3
        for (int d = 0; d < 42; d++) {
            perDocK[numDocs++] = K;
            for (int k = 0; k < K; k++) {
                ords[pos++] = tuple[k];
            }
        }
        // One more doc, partial: contributes 2 ords (positions 0..1 of K=3)
        perDocK[numDocs++] = K;
        ords[pos++] = tuple[0];
        ords[pos++] = tuple[1];
        tailMissing = 1;
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
        // tsid_A: 20 docs of [100, 200, 300] = 60 ords
        // tsid_B: 22 docs of [400, 500, 600] = 66 ords
        // total 126 ords, plus 2 of next doc of B (partial tail)
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
        // partial tail doc: 2 of 3 ords of another tupleB doc
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
        // tsid_A: K=4, 16 docs = 64 ords
        // tsid_B: K=2, 30 docs = 60 ords
        // total 124 ords; pad with one more partial K=4 doc taking 4 ords. Actually let's keep
        // it exact: tsid_A 16 docs K=4 = 64 + tsid_B 32 docs K=2 = 64. total 128, no straddle.
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
        // First doc has K=4 with headOffset=2; only positions [2,3] of its tuple are in this block.
        // Then 41 more docs of the same tuple (full K=4). Then 1 more partial for tailMissing=2.
        long[] tuple = { 11L, 22L, 33L, 44L };
        int K = tuple.length;
        long[] ords = new long[128];
        int[] perDocK = new int[129];
        int numDocs = 0;
        int pos = 0;
        // Head-partial doc: positions [2,3] = 2 ords
        perDocK[numDocs++] = K;
        ords[pos++] = tuple[2];
        ords[pos++] = tuple[3];
        // 31 full docs of K=4 = 124 ords; total so far 126
        for (int d = 0; d < 31; d++) {
            perDocK[numDocs++] = K;
            for (long o : tuple) {
                ords[pos++] = o;
            }
        }
        // Tail-partial doc: positions [0,1] = 2 ords
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
        // Block has only 50 real ords (10 docs of K=5). Rest is decoder padded zeros.
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
        // remaining positions in ords[] are 0 (matches what TSDBDocValuesBlockWriter pads)

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
        // First 50 positions match; rest are zero-padded by the decoder.
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
        out.writeVInt(0);  // headOffset
        out.writeVInt(0);  // tailMissing
        out.writeVInt(1);  // nRuns
        out.writeVInt(3);  // K
        out.writeVInt(0);  // runLen = 0 is invalid (must be >= 1)
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
        out.writeVInt(0); // K = 0 is invalid
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
        out.writeVInt(2);     // K=2
        out.writeVInt(200);   // runLen=200 => 200*2 = 400 ords would overflow 128
        out.writeVLong(0L);
        out.writeVLong(0L);
        long[] decoded = new long[128];
        expectThrows(
            CorruptIndexException.class,
            () -> TupleRunCodec.INSTANCE.decodePayload(new ByteBuffersDataInput(out.toBufferList()), decoded)
        );
    }

    public void testCyclicEmissionWithBothHeadAndTailPartialOnSingleRun() throws Exception {
        // Single tuple [a,b,c,d], one logical run that has BOTH headOffset and tailMissing.
        // Block emits a sequence of cyclic ords starting from position headOffset.
        long[] tuple = { 7L, 14L, 21L, 28L };
        int K = tuple.length;
        int headOffset = 1;
        int tailMissing = 2;
        // Total ords emitted = runLen*K - headOffset - tailMissing. Build the expected output.
        int runLen = 33; // 33 * 4 - 1 - 2 = 129. Too big. Use 32 -> 32*4 - 1 - 2 = 125.
        runLen = 32;
        int totalOrds = runLen * K - headOffset - tailMissing;
        assertEquals(125, totalOrds);
        long[] expected = new long[128];
        int cursor = headOffset;
        for (int i = 0; i < totalOrds; i++) {
            expected[i] = tuple[cursor];
            cursor++;
            if (cursor == K) cursor = 0;
        }

        // Encode by writing the wire format directly (bypasses encodePayload because we want
        // a controlled single run with both head/tail partial markers).
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
        // Positions [totalOrds..128) remain zero from the pre-fill.
        Arrays.fill(expected, totalOrds, 128, 0L);
        assertArrayEquals(expected, decoded);
    }
}
