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
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;

import java.io.IOException;
import java.util.Arrays;

public class AlpRdFloatTransformEncodeStageTests extends NumericCodecStageTestCase {

    private static final TransformDecoder DECODER = new AlpRdFloatTransformDecodeStage();

    public void testRoundTripDecimalPath() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(10.0f + i * 0.01f);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testRoundTripAlpRdPath() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt((float) Math.sqrt(i + 2));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testRoundTripConstantValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, NumericUtils.floatToSortableInt(42.5f));
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testRoundTripMixedWithExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(10.0f + i * 0.01f);
        }
        values[0] = NumericUtils.floatToSortableInt((float) Math.PI);
        values[blockSize / 3] = NumericUtils.floatToSortableInt((float) Math.E);
        values[2 * blockSize / 3] = NumericUtils.floatToSortableInt((float) Math.sqrt(2));
        values[blockSize - 1] = NumericUtils.floatToSortableInt((float) Math.sqrt(3));
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testExceptionAtPositionZero() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = NumericUtils.floatToSortableInt((float) Math.PI);
        for (int i = 1; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(10.0f + i * 0.01f);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testConsecutiveExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(10.0f + i * 0.01f);
        }
        values[5] = NumericUtils.floatToSortableInt((float) Math.PI);
        values[6] = NumericUtils.floatToSortableInt((float) Math.E);
        values[7] = NumericUtils.floatToSortableInt((float) Math.sqrt(2));
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testExceptionsAtEndOfBlock() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(10.0f + i * 0.01f);
        }
        values[blockSize - 3] = NumericUtils.floatToSortableInt((float) Math.PI);
        values[blockSize - 2] = NumericUtils.floatToSortableInt((float) Math.E);
        values[blockSize - 1] = NumericUtils.floatToSortableInt((float) Math.sqrt(2));
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testRoundTripSimilarMagnitudeFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(1000.0f + i * 0.001f * (float) Math.PI);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testSkipsWhenNoCompressionBenefit() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt((float) randomDoubleBetween(-1e7, 1e7, true));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testRoundTripRandomFloatsFuzz() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt((float) randomDoubleBetween(-1e6, 1e6, true));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testMixedPositiveNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            float v = Math.round(randomDoubleBetween(-99.99, 99.99, true) * 100.0) / 100.0f;
            values[i] = NumericUtils.floatToSortableInt(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testAllNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            float v = Math.round(randomDoubleBetween(-99.99, -0.01, true) * 100.0) / 100.0f;
            values[i] = NumericUtils.floatToSortableInt(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testZeroCrossingValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            float v = -1.0f + i * (2.0f / blockSize);
            values[i] = NumericUtils.floatToSortableInt(Math.round(v * 1000.0f) / 1000.0f);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testSubnormalFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(Float.MIN_VALUE * (i + 1));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testHighExceptionRate() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt((float) (Math.sqrt(i + 2) * Math.PI + Math.E * (i + 1)));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_FLOAT_STAGE.id,
            new AlpRdFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 8
        );
    }

    public void testRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = NumericUtils.floatToSortableInt((float) randomDoubleBetween(-1e6, 1e6, true));
            }
            assertRoundTrip(
                values,
                blockSize,
                StageId.ALP_RD_FLOAT_STAGE.id,
                new AlpRdFloatTransformEncodeStage(blockSize),
                DECODER,
                blockSize * 8
            );
        }
    }

}
