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

public class OrdinalEncodingSizeComparisonTests extends ESTestCase {

    private static final int BLOCK_SIZE = ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;

    private final RunLengthOrdinalEncoder encoder = new RunLengthOrdinalEncoder(BLOCK_SIZE);
    private final TSDBDocValuesEncoder fourStrategyEncoder = new TSDBDocValuesEncoder(BLOCK_SIZE);

    public void testFourRunsBeatsFourStrategy() throws IOException {
        final long[] block = runs(new long[] { 10, 20, 30, 40 });
        assertEncodedBytes(block, 9, 97);
    }

    public void testEightRunsBeatsFourStrategy() throws IOException {
        final long[] block = runs(new long[] { 0, 1, 2, 3, 4, 5, 6, 7 });
        assertEncodedBytes(block, 17, 49);
    }

    public void testAllDistinctMatchesFourStrategy() throws IOException {
        final long[] block = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            block[i] = i;
        }
        final long bitPacked = 1 + (long) BLOCK_SIZE * PackedInts.bitsRequired(BLOCK_SIZE - 1) / 8;
        assertEncodedBytes(block, bitPacked, bitPacked);
    }

    public void testSingleRunMatchesFourStrategy() throws IOException {
        final long[] block = new long[BLOCK_SIZE];
        Arrays.fill(block, 63);
        assertEncodedBytes(block, 1, 1);
    }

    public void testTwoRunMatchesFourStrategy() throws IOException {
        final long[] block = new long[BLOCK_SIZE];
        Arrays.fill(block, 7);
        block[0] = 3;
        assertEncodedBytes(block, 3, 3);
    }

    public void testCyclicInterleaveMatchesFourStrategy() throws IOException {
        final long[] block = new long[BLOCK_SIZE];
        Arrays.setAll(block, i -> i % 4);
        assertEncodedBytes(block, 5, 5);
    }

    public void testMultiValueSetMatchesFourStrategy() throws IOException {
        final long[] set = { 100, 200, 300, 400, 480 };
        final long[] block = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            block[i] = set[i % set.length];
        }
        assertEncodedBytes(block, 10, 10);
    }

    private void assertEncodedBytes(long[] block, long expectedRunLengthBytes, long expectedFourStrategyBytes) throws IOException {
        assertEquals("run-length bytes", expectedRunLengthBytes, encodedBytes(block, true));
        assertEquals("four-strategy bytes", expectedFourStrategyBytes, encodedBytes(block, false));
    }

    private long encodedBytes(long[] block, boolean runLength) throws IOException {
        final int bitsPerOrd = PackedInts.bitsRequired(maxOrd(block));
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        // clone: the bit-packed branch mutates its input through `forUtil`
        if (runLength) {
            encoder.encode(block.clone(), out, bitsPerOrd);
        } else {
            fourStrategyEncoder.encodeOrdinals(block.clone(), out, bitsPerOrd);
        }
        return out.size();
    }

    private static long[] runs(long[] values) {
        final long[] block = new long[BLOCK_SIZE];
        final int runLength = BLOCK_SIZE / values.length;
        for (int i = 0; i < values.length; i++) {
            Arrays.fill(block, i * runLength, (i + 1) * runLength, values[i]);
        }
        return block;
    }

    private static long maxOrd(long[] block) {
        long max = 0;
        for (long value : block) {
            max = Math.max(max, value);
        }
        return max;
    }
}
