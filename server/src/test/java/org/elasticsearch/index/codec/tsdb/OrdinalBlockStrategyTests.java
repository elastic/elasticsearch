/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1";
 */
package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.codec.tsdb.OrdinalBlockStrategy.Stats;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class OrdinalBlockStrategyTests extends ESTestCase {

    private static final int BLOCK_SIZE = 128;

    private final DocValuesForUtil forUtil = new DocValuesForUtil(BLOCK_SIZE);

    public void testSingleRunRoundTrips() throws IOException {
        final long[] block = new long[BLOCK_SIZE];
        Arrays.fill(block, 63);
        assertRoundTrips(OrdinalBlockStrategy.SingleRun.INSTANCE, block);
    }

    public void testSingleRunNotApplicableForTwoRuns() {
        final long[] block = new long[BLOCK_SIZE];
        Arrays.fill(block, 5);
        block[0] = 1;
        assertEquals(OrdinalBlockStrategy.NOT_APPLICABLE, encodedSize(OrdinalBlockStrategy.SingleRun.INSTANCE, block));
    }

    public void testTwoRunRoundTrips() throws IOException {
        final long[] block = new long[BLOCK_SIZE];
        Arrays.fill(block, 7);
        block[0] = 3;
        assertRoundTrips(OrdinalBlockStrategy.TwoRun.INSTANCE, block);
    }

    public void testTwoRunNotApplicableForSingleRun() {
        final long[] block = new long[BLOCK_SIZE];
        Arrays.fill(block, 9);
        assertEquals(OrdinalBlockStrategy.NOT_APPLICABLE, encodedSize(OrdinalBlockStrategy.TwoRun.INSTANCE, block));
    }

    public void testBitPackedRoundTrips() throws IOException {
        final long[] block = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            block[i] = i;
        }
        assertRoundTrips(OrdinalBlockStrategy.BitPacked.INSTANCE, block);
    }

    public void testRunLengthRoundTrips() throws IOException {
        final long[] block = new long[BLOCK_SIZE];
        final int runLength = BLOCK_SIZE / 4;
        for (int i = 0; i < BLOCK_SIZE; i++) {
            block[i] = i / runLength;
        }
        assertRoundTrips(OrdinalBlockStrategy.RunLength.INSTANCE, block);
    }

    public void testCyclicRoundTrips() throws IOException {
        final long[] block = new long[BLOCK_SIZE];
        final long[] set = { 100, 200, 300, 400, 480 };
        for (int i = 0; i < BLOCK_SIZE; i++) {
            block[i] = set[i % set.length];
        }
        assertRoundTrips(OrdinalBlockStrategy.Cyclic.INSTANCE, block);
    }

    public void testCyclicNotApplicableForRunBased() {
        final long[] block = new long[BLOCK_SIZE];
        final int runLength = BLOCK_SIZE / 4;
        for (int i = 0; i < BLOCK_SIZE; i++) {
            block[i] = i / runLength;
        }
        assertEquals(OrdinalBlockStrategy.NOT_APPLICABLE, encodedSize(OrdinalBlockStrategy.Cyclic.INSTANCE, block));
    }

    public void testRunLengthRoundTripsWithDecreasingRuns() throws IOException {
        final long[] block = new long[BLOCK_SIZE];
        final int runLength = BLOCK_SIZE / 4;
        Arrays.fill(block, 0, runLength, 100);
        Arrays.fill(block, runLength, 2 * runLength, 60);
        Arrays.fill(block, 2 * runLength, 3 * runLength, 30);
        Arrays.fill(block, 3 * runLength, BLOCK_SIZE, 10);
        assertRoundTrips(OrdinalBlockStrategy.RunLength.INSTANCE, block);
    }

    private void assertRoundTrips(OrdinalBlockStrategy strategy, long[] block) throws IOException {
        final int bitsPerOrd = PackedInts.bitsRequired(maxOrd(block));
        final Stats stats = Stats.of(block);
        final long expectedSize = strategy.encodedSize(stats, bitsPerOrd, BLOCK_SIZE);
        assertNotEquals("strategy should apply to this block", OrdinalBlockStrategy.NOT_APPLICABLE, expectedSize);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        strategy.encode(block.clone(), stats, bitsPerOrd, forUtil, out);
        assertEquals("encodedSize matches encoded length", expectedSize, out.size());

        final DataInput in = out.toDataInput();
        final long header = in.readVLong();
        final int encoding = Long.numberOfTrailingZeros(~header);
        final long[] decoded = new long[BLOCK_SIZE];
        strategy.decode(header >>> (encoding + 1), bitsPerOrd, in, decoded, forUtil);
        assertArrayEquals(block, decoded);
    }

    private long encodedSize(OrdinalBlockStrategy strategy, long[] block) {
        return strategy.encodedSize(Stats.of(block), PackedInts.bitsRequired(maxOrd(block)), BLOCK_SIZE);
    }

    private static long maxOrd(long[] block) {
        long max = 0;
        for (long value : block) {
            max = Math.max(max, value);
        }
        return max;
    }
}
