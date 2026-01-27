/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.pipeline.StageId;

import java.io.IOException;
import java.util.stream.LongStream;

public class ZigzagCodecStageTests extends NumericCodecStageTestCase {

    public void testIdAndName() {
        assertEquals((byte) 0x05, ZigzagCodecStage.INSTANCE.id());
        assertEquals("zigzag", ZigzagCodecStage.INSTANCE.name());
    }

    public void testRoundTripPositiveValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.range(0, blockSize).toArray(), blockSize);
    }

    public void testRoundTripNegativeValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.range(-blockSize, 0).toArray(), blockSize);
    }

    public void testRoundTripMixedValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.range(-blockSize / 2, blockSize / 2).toArray(), blockSize);
    }

    public void testRoundTripRandomValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.generate(ZigzagCodecStageTests::randomLong).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripSmallDeltas() throws IOException {
        final int blockSize = randomBlockSize();
        long[] values = new long[blockSize];
        long current = randomLongBetween(-1000, 1000);
        for (int i = 0; i < blockSize; i++) {
            values[i] = current;
            current += randomLongBetween(-5, 5);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripAlternatingSign() throws IOException {
        final int blockSize = randomBlockSize();
        long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = (i % 2 == 0 ? 1 : -1) * randomLongBetween(1, 1000);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripAllZeros() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[blockSize], blockSize);
    }

    public void testRoundTripAllSameValue() throws IOException {
        final int blockSize = randomBlockSize();
        final long value = randomLong();
        assertRoundTrip(LongStream.generate(() -> value).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripEdgeCaseValues() throws IOException {
        final int blockSize = randomBlockSize();
        long[] values = new long[blockSize];
        values[0] = Long.MAX_VALUE;
        values[1] = Long.MIN_VALUE;
        values[2] = 0;
        values[3] = 1;
        values[4] = -1;
        for (int i = 5; i < blockSize; i++) {
            values[i] = randomLong();
        }
        assertRoundTrip(values, blockSize);
    }

    private void assertRoundTrip(final long[] original, int blockSize) throws IOException {
        assertRoundTrip(original, blockSize, StageId.ZIGZAG.id, ZigzagCodecStage.INSTANCE);
    }
}
