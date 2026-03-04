/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class DocOffsetsCodecTests extends ESTestCase {

    public void testSimpleBitpacking() throws IOException {
        int[] offsets = { 0, 10, 20, 30, 40 };
        int numDocs = offsets.length - 1;
        assertRoundTrip(DocOffsetsCodec.BITPACKING, offsets, numDocs);
    }

    public void testSimpleGroupedVint() throws IOException {
        int[] offsets = { 0, 10, 20, 30, 40 };
        int numDocs = offsets.length - 1;
        assertRoundTrip(DocOffsetsCodec.GROUPED_VINT, offsets, numDocs);
    }

    public void testUniformDeltaBitpacking() throws IOException {
        int[] offsets = { 0, 10, 20, 30, 40, 50 };
        int numDocs = offsets.length - 1;
        assertRoundTrip(DocOffsetsCodec.BITPACKING, offsets, numDocs);
    }

    public void testUniformDeltaGroupedVint() throws IOException {
        int[] offsets = { 0, 10, 20, 30, 40, 50 };
        int numDocs = offsets.length - 1;
        assertRoundTrip(DocOffsetsCodec.GROUPED_VINT, offsets, numDocs);
    }

    public void testZeroDeltasBitpacking() throws IOException {
        int[] offsets = { 0, 0, 0, 0, 0 };
        int numDocs = offsets.length - 1;
        assertRoundTrip(DocOffsetsCodec.BITPACKING, offsets, numDocs);
    }

    public void testZeroDeltasGroupedVint() throws IOException {
        int[] offsets = { 0, 0, 0, 0, 0 };
        int numDocs = offsets.length - 1;
        assertRoundTrip(DocOffsetsCodec.GROUPED_VINT, offsets, numDocs);
    }

    public void testSingleDocBitpacking() throws IOException {
        int[] offsets = { 0, 42 };
        assertRoundTrip(DocOffsetsCodec.BITPACKING, offsets, 1);
    }

    public void testSingleDocGroupedVint() throws IOException {
        int[] offsets = { 0, 42 };
        assertRoundTrip(DocOffsetsCodec.GROUPED_VINT, offsets, 1);
    }

    public void testRandomBitpacking() throws IOException {
        for (int iter = 0; iter < 100; iter++) {
            int numDocs = between(1, 256);
            int[] offsets = generateMonotonicOffsets(numDocs);
            assertRoundTrip(DocOffsetsCodec.BITPACKING, offsets, numDocs);
        }
    }

    public void testRandomGroupedVint() throws IOException {
        for (int iter = 0; iter < 100; iter++) {
            int numDocs = between(1, 256);
            int[] offsets = generateMonotonicOffsets(numDocs);
            assertRoundTrip(DocOffsetsCodec.GROUPED_VINT, offsets, numDocs);
        }
    }

    /**
     * Generates a monotonically non-decreasing array of offsets with numDocs + 1 entries.
     */
    private int[] generateMonotonicOffsets(int numDocs) {
        int numOffsets = numDocs + 1;
        int[] offsets = new int[numOffsets];
        offsets[0] = 0;
        for (int i = 1; i < numOffsets; i++) {
            offsets[i] = offsets[i - 1] + between(0, 1000);
        }
        return offsets;
    }

    private void assertRoundTrip(DocOffsetsCodec codec, int[] originalOffsets, int numDocs) throws IOException {
        int numOffsets = numDocs + 1;

        byte[] buffer = new byte[numOffsets * 8 + 64];
        ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

        // encode mutates the array in-place, so pass a copy
        int[] encodeInput = Arrays.copyOf(originalOffsets, numOffsets);
        codec.getEncoder().encode(encodeInput, numDocs, out);

        int written = out.getPosition();
        ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, written);

        int[] decoded = new int[numOffsets];
        codec.getDecoder().decode(decoded, numDocs, in);

        assertArrayEquals(
            "Round-trip failed for " + codec.name() + " with numDocs=" + numDocs + ": expected " + Arrays.toString(originalOffsets),
            originalOffsets,
            Arrays.copyOf(decoded, numOffsets)
        );
    }
}
