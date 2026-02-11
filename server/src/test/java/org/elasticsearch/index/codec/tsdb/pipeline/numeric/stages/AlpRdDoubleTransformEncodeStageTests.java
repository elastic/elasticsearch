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
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;

import java.io.IOException;
import java.util.Arrays;

public class AlpRdDoubleTransformEncodeStageTests extends NumericCodecStageTestCase {

    private static final TransformDecoder DECODER = new AlpRdDoubleTransformDecodeStage();

    public void testRoundTripDecimalPath() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testRoundTripAlpRdPath() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Math.sqrt(i + 2));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testRoundTripConstantValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, NumericUtils.doubleToSortableLong(42.5));
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testRoundTripMixedWithExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        values[0] = NumericUtils.doubleToSortableLong(Math.PI);
        values[blockSize / 3] = NumericUtils.doubleToSortableLong(Math.E);
        values[2 * blockSize / 3] = NumericUtils.doubleToSortableLong(Math.sqrt(2));
        values[blockSize - 1] = NumericUtils.doubleToSortableLong(Math.sqrt(3));
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testExceptionAtPositionZero() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = NumericUtils.doubleToSortableLong(Math.PI);
        for (int i = 1; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testConsecutiveExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        values[5] = NumericUtils.doubleToSortableLong(Math.PI);
        values[6] = NumericUtils.doubleToSortableLong(Math.E);
        values[7] = NumericUtils.doubleToSortableLong(Math.sqrt(2));
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testExceptionsAtEndOfBlock() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        values[blockSize - 3] = NumericUtils.doubleToSortableLong(Math.PI);
        values[blockSize - 2] = NumericUtils.doubleToSortableLong(Math.E);
        values[blockSize - 1] = NumericUtils.doubleToSortableLong(Math.sqrt(2));
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testRoundTripSimilarMagnitudeDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(1000.0 + i * 0.001 * Math.PI);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testSkipsWhenNoCompressionBenefit() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(-1e15, 1e15, true));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testFullPipelineRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Math.sqrt(i + 2));
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(
            PipelineConfig.forDoubles(blockSize).alpRdDoubleStage().offset().gcd().bitPack()
        );
        assertFullPipelineRoundTrip(values, encoder);
    }

    public void testRoundTripRandomDoublesFuzz() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(-1e6, 1e6, true));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testMixedPositiveNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            double v = Math.round(randomDoubleBetween(-99.99, 99.99, true) * 100.0) / 100.0;
            values[i] = NumericUtils.doubleToSortableLong(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testAllNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            double v = Math.round(randomDoubleBetween(-99.99, -0.01, true) * 100.0) / 100.0;
            values[i] = NumericUtils.doubleToSortableLong(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testZeroCrossingValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            double v = -1.0 + i * (2.0 / blockSize);
            values[i] = NumericUtils.doubleToSortableLong(Math.round(v * 1000.0) / 1000.0);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testSubnormalDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            double v = Double.MIN_VALUE * (i + 1);
            values[i] = NumericUtils.doubleToSortableLong(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testHighExceptionRate() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Math.sqrt(i + 2) * Math.PI + Math.E * (i + 1));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_RD_DOUBLE_STAGE.id,
            new AlpRdDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(-1e12, 1e12, true));
            }
            assertRoundTrip(
                values,
                blockSize,
                StageId.ALP_RD_DOUBLE_STAGE.id,
                new AlpRdDoubleTransformEncodeStage(blockSize),
                DECODER,
                blockSize * 12
            );
        }
    }

}
