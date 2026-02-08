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

public class DeltaCodecStageTests extends NumericCodecStageTestCase {

    public void testIdAndName() {
        assertEquals((byte) 0x01, DeltaCodecStage.INSTANCE.id());
        assertEquals("delta", DeltaCodecStage.INSTANCE.name());
    }

    public void testRoundTripMonotonicIncreasing() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = randomLongBetween(0, 1000);
        final long increment = randomLongBetween(1, 100);
        assertRoundTrip(LongStream.iterate(start, v -> v + increment).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripMonotonicDecreasing() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = randomLongBetween(1000, 10000);
        final long decrement = randomLongBetween(1, 100);
        assertRoundTrip(LongStream.iterate(start, v -> v - decrement).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripWithNegativeValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = randomLongBetween(-10000, -1000);
        final long increment = randomLongBetween(1, 100);
        assertRoundTrip(LongStream.iterate(start, v -> v + increment).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripLargeValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = Long.MAX_VALUE - blockSize * 100L;
        assertRoundTrip(LongStream.iterate(start, v -> v + randomLongBetween(1, 50)).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripNonMonotonic() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.generate(() -> randomLongBetween(-1000, 1000)).limit(blockSize).toArray(), blockSize);
    }

    public void testSkipNonMonotonic() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = (i % 2 == 0) ? 0 : 1;
        }
        assertStageSkipped(values, blockSize, StageId.DELTA.id, DeltaCodecStage.INSTANCE);
    }

    public void testRoundTripAllSame() throws IOException {
        final int blockSize = randomBlockSize();
        final long value = randomLong();
        assertRoundTrip(LongStream.generate(() -> value).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripTwoValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[] { randomLongBetween(0, 100), randomLongBetween(101, 200) }, blockSize);
    }

    private void assertRoundTrip(final long[] original, int blockSize) throws IOException {
        assertRoundTrip(original, blockSize, StageId.DELTA.id, DeltaCodecStage.INSTANCE);
    }
}
