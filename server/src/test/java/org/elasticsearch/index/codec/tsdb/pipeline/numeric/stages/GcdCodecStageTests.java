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

public class GcdCodecStageTests extends AbstractTransformStageTestCase {

    public void testIdMatchesStageId() {
        assertEquals(StageId.GCD_STAGE.id, GcdCodecStage.INSTANCE.id());
    }

    public void testSkipsWhenGcdIsOne() {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = 7;
        values[1] = 11;
        for (int i = 2; i < blockSize; i++) {
            values[i] = randomLongBetween(1, 10000);
        }

        assertStageSkipped(GcdCodecStage.INSTANCE, values, blockSize);
    }

    public void testRoundTrip() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final int blockSize = randomBlockSize();
            final long gcd = randomLongBetween(2, 1000);
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = gcd * randomLongBetween(1, 10000);
            }
            assertTransformRoundTrip(GcdCodecStage.INSTANCE, values);
        }
    }

    public void testRoundTripPowerOfTwoGcd() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final int blockSize = randomBlockSize();
            final long gcd = 1L << randomIntBetween(1, 30);
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = gcd * randomLongBetween(1, 10000);
            }
            assertTransformRoundTrip(GcdCodecStage.INSTANCE, values);
        }
    }

    public void testRoundTripLargeGcd() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final int blockSize = randomBlockSize();
            final long gcd = randomLongBetween(100_000, 1_000_000_000L);
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = gcd * randomLongBetween(1, 1000);
            }
            assertTransformRoundTrip(GcdCodecStage.INSTANCE, values);
        }
    }

    public void testSkipsWhenAllZeros() {
        final int blockSize = randomBlockSize();
        assertStageSkipped(GcdCodecStage.INSTANCE, new long[blockSize], blockSize);
    }

    public void testRoundTripWithZeros() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final int blockSize = randomBlockSize();
            final long gcd = randomLongBetween(2, 100);
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = (i % 3 == 0) ? 0 : gcd * randomLongBetween(1, 10000);
            }
            assertTransformRoundTrip(GcdCodecStage.INSTANCE, values);
        }
    }

    public void testMultiBlockRoundTripWithArrayReuse() throws IOException {
        final int blockSize = randomBlockSize();
        final int numBlocks = randomIntBetween(2, 5);
        final long[][] blocks = new long[numBlocks][blockSize];
        for (int b = 0; b < numBlocks; b++) {
            final long gcd = randomLongBetween(2, 1000);
            for (int i = 0; i < blockSize; i++) {
                blocks[b][i] = gcd * randomLongBetween(1, 10000);
            }
        }
        assertMultiBlockTransformRoundTrip(GcdCodecStage.INSTANCE, blocks);
    }
}
