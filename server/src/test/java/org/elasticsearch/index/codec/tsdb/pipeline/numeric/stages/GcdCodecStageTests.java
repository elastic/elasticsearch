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

public class GcdCodecStageTests extends NumericCodecStageTestCase {

    public void testIdAndName() {
        assertEquals((byte) 0x03, GcdCodecStage.INSTANCE.id());
        assertEquals("gcd", GcdCodecStage.INSTANCE.name());
    }

    public void testRoundTripBasic() throws IOException {
        final int blockSize = randomBlockSize();
        final long gcd = randomLongBetween(2, 100);
        assertRoundTrip(LongStream.iterate(gcd, v -> v + gcd).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripLargeGcd() throws IOException {
        final int blockSize = randomBlockSize();
        final long gcd = randomLongBetween(100000000, 1000000000);
        assertRoundTrip(LongStream.iterate(gcd, v -> v + gcd).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripWithZeros() throws IOException {
        final int blockSize = randomBlockSize();
        final long gcd = randomLongBetween(2, 100);
        assertRoundTrip(LongStream.range(0, blockSize).map(i -> i % 3 == 0 ? 0 : (i + 1) * gcd).toArray(), blockSize);
    }

    public void testRoundTripGcdOfTwo() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.iterate(2, v -> v + 2).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripGcdEqualsOne() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = LongStream.iterate(1, v -> v + 2).limit(blockSize).toArray();
        values[1] = 2;
        assertRoundTrip(values, blockSize);
    }

    public void testSkipGcdEqualsOne() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = 2;
        values[1] = 3;
        for (int i = 2; i < blockSize; i++) {
            values[i] = 5 + i;
        }
        assertStageSkipped(values, blockSize, StageId.GCD.id, GcdCodecStage.INSTANCE);
    }

    public void testRoundTripAllZeros() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[blockSize], blockSize);
    }

    private void assertRoundTrip(final long[] original, int blockSize) throws IOException {
        assertRoundTrip(original, blockSize, StageId.GCD.id, GcdCodecStage.INSTANCE);
    }
}
