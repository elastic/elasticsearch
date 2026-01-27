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

public class OffsetCodecStageTests extends NumericCodecStageTestCase {

    public void testIdAndName() {
        assertEquals((byte) 0x02, OffsetCodecStage.INSTANCE.id());
        assertEquals("offset", OffsetCodecStage.INSTANCE.name());
    }

    public void testRoundTripPositiveOffset() throws IOException {
        final int blockSize = randomBlockSize();
        final long min = randomLongBetween(1000000, 10000000);
        final long increment = randomLongBetween(1, 10);
        assertRoundTrip(LongStream.iterate(min, v -> v + increment).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripNegativeOffset() throws IOException {
        final int blockSize = randomBlockSize();
        final long min = randomLongBetween(-10000000, -1000000);
        final long increment = randomLongBetween(1, 10);
        assertRoundTrip(LongStream.iterate(min, v -> v + increment).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripLargePositiveOffset() throws IOException {
        final int blockSize = randomBlockSize();
        final long min = Long.MAX_VALUE / 2;
        final long increment = randomLongBetween(1, 10);
        assertRoundTrip(LongStream.iterate(min, v -> v + increment).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripZeroMin() throws IOException {
        final int blockSize = randomBlockSize();
        final long increment = randomLongBetween(1, 10);
        assertRoundTrip(LongStream.iterate(0, v -> v + increment).limit(blockSize).toArray(), blockSize);
    }

    public void testSkipZeroMin() throws IOException {
        final int blockSize = randomBlockSize();
        final long increment = randomLongBetween(1, 10);
        assertStageSkipped(
            LongStream.iterate(0, v -> v + increment).limit(blockSize).toArray(),
            blockSize,
            StageId.OFFSET.id,
            OffsetCodecStage.INSTANCE
        );
    }

    public void testRoundTripOverflow() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[] { Long.MIN_VALUE, Long.MAX_VALUE }, blockSize);
    }

    public void testRoundTripSmallMinRelativeToMax() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[] { 1, randomLongBetween(1000000, 10000000) }, blockSize);
    }

    public void testSkipSmallMinRelativeToMax() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = 1;
        for (int i = 1; i < blockSize; i++) {
            values[i] = randomLongBetween(1000000, 10000000);
        }
        assertStageSkipped(values, blockSize, StageId.OFFSET.id, OffsetCodecStage.INSTANCE);
    }

    public void testRoundTripAllSamePositive() throws IOException {
        final int blockSize = randomBlockSize();
        final long value = randomLongBetween(1000000, 10000000);
        assertRoundTrip(LongStream.generate(() -> value).limit(blockSize).toArray(), blockSize);
    }

    private void assertRoundTrip(final long[] original, int blockSize) throws IOException {
        assertRoundTrip(original, blockSize, StageId.OFFSET.id, OffsetCodecStage.INSTANCE);
    }
}
