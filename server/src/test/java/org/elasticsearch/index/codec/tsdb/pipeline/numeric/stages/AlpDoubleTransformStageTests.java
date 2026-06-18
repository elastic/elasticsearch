/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;

import java.io.IOException;
import java.util.Arrays;

public class AlpDoubleTransformStageTests extends AbstractTransformStageTestCase {

    public void testIdMatchesStageId() {
        assertEquals(StageId.ALP_DOUBLE_STAGE.id, new AlpDoubleTransformStage(128).id());
    }

    public void testConstructorRejectsNonPositiveBlockSize() {
        expectThrows(IllegalArgumentException.class, () -> new AlpDoubleTransformStage(0));
        expectThrows(IllegalArgumentException.class, () -> new AlpDoubleTransformStage(-1));
    }

    public void testRoundTrip2dpDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripConstantDecimal() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, NumericUtils.doubleToSortableLong(42.5));
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripIntegerLikeDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) randomIntBetween(0, 10_000));
        }
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripSensorLikeData() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        final double base = 22.5;
        for (int i = 0; i < blockSize; i++) {
            final double v = base + Math.round(randomDoubleBetween(-0.5, 0.5, true) * 10.0) / 10.0;
            values[i] = NumericUtils.doubleToSortableLong(v);
        }
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripMixedPositiveNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            final double v = Math.round(randomDoubleBetween(-99.99, 99.99, true) * 100.0) / 100.0;
            values[i] = NumericUtils.doubleToSortableLong(v);
        }
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripAllNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            final double v = Math.round(randomDoubleBetween(-99.99, -0.01, true) * 100.0) / 100.0;
            values[i] = NumericUtils.doubleToSortableLong(v);
        }
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripZeroCrossingValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            final double v = -1.0 + i * (2.0 / blockSize);
            values[i] = NumericUtils.doubleToSortableLong(Math.round(v * 1000.0) / 1000.0);
        }
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripMixedPrecisionWithExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            if (i % 10 == 0) {
                values[i] = NumericUtils.doubleToSortableLong(Math.PI * (i + 1));
            } else {
                values[i] = NumericUtils.doubleToSortableLong(Math.round(randomDoubleBetween(0.1, 100.0, true) * 100.0) / 100.0);
            }
        }
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripExceptionAtPositionZero() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = NumericUtils.doubleToSortableLong(Math.PI);
        for (int i = 1; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripConsecutiveExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        values[5] = NumericUtils.doubleToSortableLong(Math.PI);
        values[6] = NumericUtils.doubleToSortableLong(Math.E);
        values[7] = NumericUtils.doubleToSortableLong(Math.sqrt(2));
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripExceptionsAtEndOfBlock() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        values[blockSize - 3] = NumericUtils.doubleToSortableLong(Math.PI);
        values[blockSize - 2] = NumericUtils.doubleToSortableLong(Math.E);
        values[blockSize - 1] = NumericUtils.doubleToSortableLong(Math.sqrt(2));
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripSubnormalDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Double.MIN_VALUE * (i + 1));
        }
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripHighExceptionRateFallsThrough() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Math.sqrt(i + 2) * Math.PI);
        }
        assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
    }

    public void testRoundTripRandomDoublesFuzz() throws IOException {
        for (int iter = 0; iter < 25; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(-1e12, 1e12, true));
            }
            assertTransformRoundTrip(new AlpDoubleTransformStage(blockSize), values);
        }
    }

    public void testMultiBlockRoundTripWithArrayReuse() throws IOException {
        final int blockSize = randomBlockSize();
        final int numBlocks = randomIntBetween(2, 5);
        final long[][] blocks = new long[numBlocks][blockSize];
        for (int b = 0; b < numBlocks; b++) {
            for (int i = 0; i < blockSize; i++) {
                blocks[b][i] = NumericUtils.doubleToSortableLong(10.0 + (b * blockSize + i) * 0.01);
            }
        }
        assertMultiBlockTransformRoundTrip(new AlpDoubleTransformStage(blockSize), blocks);
    }
}
