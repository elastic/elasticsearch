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
import java.util.Arrays;

public class DeltaOfDeltaCodecStageTests extends AbstractTransformStageTestCase {

    public void testIdMatchesStageId() {
        assertEquals(StageId.DELTA_OF_DELTA_STAGE.id, DeltaOfDeltaCodecStage.INSTANCE.id());
    }

    public void testNonMonotonicSkips() {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = randomLong();
        }
        values[0] = 0;
        values[1] = 100;
        values[2] = 200;
        values[3] = 50;
        values[4] = -10;

        assertStageSkipped(DeltaOfDeltaCodecStage.INSTANCE, values, blockSize);
    }

    public void testAllSameValuesSkips() {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, randomLong());

        assertStageSkipped(DeltaOfDeltaCodecStage.INSTANCE, values, blockSize);
    }

    public void testTooFewMonotonicValuesSkips() {
        assertStageSkipped(DeltaOfDeltaCodecStage.INSTANCE, new long[] { 10, 20 }, 2);
        assertStageSkipped(DeltaOfDeltaCodecStage.INSTANCE, new long[] { 10, 20, 20 }, 3);
    }

    public void testRoundTripMonotonicIncreasing() throws IOException {
        for (int i = 0; i < 10; i++) {
            assertTransformRoundTrip(DeltaOfDeltaCodecStage.INSTANCE, randomMonotonicIncreasing(randomBlockSize()));
        }
    }

    public void testRoundTripMonotonicDecreasing() throws IOException {
        for (int i = 0; i < 10; i++) {
            assertTransformRoundTrip(DeltaOfDeltaCodecStage.INSTANCE, randomMonotonicDecreasing(randomBlockSize()));
        }
    }

    public void testRoundTripConstantInterval() throws IOException {
        final int blockSize = randomBlockSize();
        final long base = randomLongBetween(0, 1_000_000);
        final long interval = randomLongBetween(1, 10_000);
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = base + (long) i * interval;
        }
        assertTransformRoundTrip(DeltaOfDeltaCodecStage.INSTANCE, values);
    }

    public void testRoundTripConstantRateOfChange() throws IOException {
        final int blockSize = randomBlockSize();
        final long base = randomLongBetween(0, 1_000_000);
        final long firstInterval = randomLongBetween(1, 10_000);
        final long rateOfChange = randomLongBetween(1, 100);
        final long[] values = new long[blockSize];
        values[0] = base;
        long interval = firstInterval;
        for (int i = 1; i < blockSize; i++) {
            values[i] = values[i - 1] + interval;
            interval += rateOfChange;
        }
        assertTransformRoundTrip(DeltaOfDeltaCodecStage.INSTANCE, values);
    }

    public void testMultiBlockRoundTripWithArrayReuse() throws IOException {
        final int blockSize = randomBlockSize();
        final int numBlocks = randomIntBetween(2, 5);
        final long[][] blocks = new long[numBlocks][];
        for (int b = 0; b < numBlocks; b++) {
            blocks[b] = randomBoolean() ? randomMonotonicIncreasing(blockSize) : randomMonotonicDecreasing(blockSize);
        }
        assertMultiBlockTransformRoundTrip(DeltaOfDeltaCodecStage.INSTANCE, blocks);
    }
}
