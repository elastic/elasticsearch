/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class RunLengthOrdinalEncodingRoundTripTests extends ESTestCase {

    public void testSingleRun() throws IOException {
        final long[] block = new long[randomBlockSize()];
        Arrays.fill(block, 63);
        assertRoundTrips(block);
    }

    public void testTwoRuns() throws IOException {
        final long[] block = new long[randomBlockSize()];
        Arrays.fill(block, 7);
        block[0] = 3;
        assertRoundTrips(block);
    }

    public void testFourRuns() throws IOException {
        final long[] block = new long[randomBlockSize()];
        final int runLength = block.length / 4;
        Arrays.fill(block, 0, runLength, 10);
        Arrays.fill(block, runLength, 2 * runLength, 20);
        Arrays.fill(block, 2 * runLength, 3 * runLength, 30);
        Arrays.fill(block, 3 * runLength, block.length, 40);
        assertRoundTrips(block);
    }

    public void testLargeValues() throws IOException {
        final long[] block = new long[randomBlockSize()];
        Arrays.fill(block, 0, block.length / 2, Long.MAX_VALUE >> 2);
        Arrays.fill(block, block.length / 2, block.length, Long.MAX_VALUE >> 1);
        assertRoundTrips(block);
    }

    public void testAllDistinct() throws IOException {
        final long[] block = new long[randomBlockSize()];
        for (int i = 0; i < block.length; i++) {
            block[i] = i;
        }
        assertRoundTrips(block);
    }

    public void testMultiValueInterleave() throws IOException {
        final long[] set = { 100, 200, 300, 400, 480 };
        final long[] block = new long[randomBlockSize()];
        for (int i = 0; i < block.length; i++) {
            block[i] = set[i % set.length];
        }
        assertRoundTrips(block);
    }

    public void testRandomOrdinals() throws IOException {
        final long[] block = new long[randomBlockSize()];
        final int cardinality = randomIntBetween(1, 1 << 16);
        for (int i = 0; i < block.length; i++) {
            block[i] = randomIntBetween(0, cardinality - 1);
        }
        assertRoundTrips(block);
    }

    private void assertRoundTrips(long[] block) throws IOException {
        final int blockSize = block.length;
        final int bitsPerOrd = PackedInts.bitsRequired(maxOrd(block));
        final long[] expected = block.clone();
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        new RunLengthOrdinalEncoder(blockSize).encode(block, out, bitsPerOrd);
        final long[] decoded = new long[blockSize];
        new RunLengthOrdinalDecoder(blockSize).decode(out.toDataInput(), decoded, bitsPerOrd);
        assertArrayEquals(expected, decoded);
    }

    // a power of two between 128 and 4096, the range of per-field ordinal block sizes
    private static int randomBlockSize() {
        return 1 << randomIntBetween(7, 12);
    }

    private static long maxOrd(long[] block) {
        long max = 0;
        for (long value : block) {
            max = Math.max(max, value);
        }
        return max;
    }
}
