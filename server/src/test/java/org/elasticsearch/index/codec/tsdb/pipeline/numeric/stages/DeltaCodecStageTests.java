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

public class DeltaCodecStageTests extends AbstractTransformStageTestCase {

    public void testIdMatchesStageId() {
        assertEquals(StageId.DELTA_STAGE.id, DeltaCodecStage.INSTANCE.id());
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

        assertStageSkipped(DeltaCodecStage.INSTANCE, values, blockSize);
    }

    public void testAllSameValuesSkips() {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, randomLong());

        assertStageSkipped(DeltaCodecStage.INSTANCE, values, blockSize);
    }

    public void testTooFewMonotonicValuesSkips() {
        assertStageSkipped(DeltaCodecStage.INSTANCE, new long[] { 10, 20 }, 2);
        assertStageSkipped(DeltaCodecStage.INSTANCE, new long[] { 10, 20, 20 }, 3);
    }

    public void testRoundTripMonotonicIncreasing() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            assertTransformRoundTrip(DeltaCodecStage.INSTANCE, randomMonotonicIncreasing(randomBlockSize()));
        }
    }

    public void testRoundTripMonotonicDecreasing() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            assertTransformRoundTrip(DeltaCodecStage.INSTANCE, randomMonotonicDecreasing(randomBlockSize()));
        }
    }

    public void testMultiBlockRoundTripWithArrayReuse() throws IOException {
        final int blockSize = randomBlockSize();
        final int numBlocks = randomIntBetween(2, 5);
        final long[][] blocks = new long[numBlocks][];
        for (int b = 0; b < numBlocks; b++) {
            blocks[b] = randomBoolean() ? randomMonotonicIncreasing(blockSize) : randomMonotonicDecreasing(blockSize);
        }
        assertMultiBlockTransformRoundTrip(DeltaCodecStage.INSTANCE, blocks);
    }
}
