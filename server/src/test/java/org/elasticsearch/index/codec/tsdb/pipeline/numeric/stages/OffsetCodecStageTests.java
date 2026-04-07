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

public class OffsetCodecStageTests extends AbstractTransformStageTestCase {

    public void testIdMatchesStageId() {
        assertEquals(StageId.OFFSET_STAGE.id, OffsetCodecStage.INSTANCE.id());
    }

    public void testSkipsWhenMinIsZero() {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = 0;
        for (int i = 1; i < blockSize; i++) {
            values[i] = randomLongBetween(0, 1000);
        }

        assertStageSkipped(OffsetCodecStage.INSTANCE, values, blockSize);
    }

    public void testSkipsWhenMinSmallRelativeToMax() {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = 1;
        for (int i = 1; i < blockSize; i++) {
            values[i] = randomLongBetween(100, 1000);
        }

        assertStageSkipped(OffsetCodecStage.INSTANCE, values, blockSize);
    }

    public void testSkipsOnOverflow() {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = Long.MIN_VALUE;
        values[1] = Long.MAX_VALUE;
        for (int i = 2; i < blockSize; i++) {
            values[i] = randomLong();
        }

        assertStageSkipped(OffsetCodecStage.INSTANCE, values, blockSize);
    }

    public void testRoundTrip() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final int blockSize = randomBlockSize();
            final long base = randomLongBetween(10000, Long.MAX_VALUE / 2);
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = base + randomLongBetween(0, 100);
            }
            assertTransformRoundTrip(OffsetCodecStage.INSTANCE, values);
        }
    }

    public void testRoundTripNegativeMin() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final int blockSize = randomBlockSize();
            final long base = randomLongBetween(Long.MIN_VALUE / 2, -10000);
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = base + randomLongBetween(0, 100);
            }
            assertTransformRoundTrip(OffsetCodecStage.INSTANCE, values);
        }
    }

    public void testMultiBlockRoundTripWithArrayReuse() throws IOException {
        final int blockSize = randomBlockSize();
        final int numBlocks = randomIntBetween(2, 5);
        final long[][] blocks = new long[numBlocks][blockSize];
        for (int b = 0; b < numBlocks; b++) {
            final long base = randomLongBetween(10000, Long.MAX_VALUE / 2);
            for (int i = 0; i < blockSize; i++) {
                blocks[b][i] = base + randomLongBetween(0, 100);
            }
        }
        assertMultiBlockTransformRoundTrip(OffsetCodecStage.INSTANCE, blocks);
    }
}
