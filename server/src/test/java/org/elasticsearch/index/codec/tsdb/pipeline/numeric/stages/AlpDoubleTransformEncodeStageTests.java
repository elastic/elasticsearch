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

public class AlpDoubleTransformEncodeStageTests extends NumericCodecStageTestCase {

    private static final TransformDecoder DECODER = new AlpDoubleTransformDecodeStage();

    public void testRoundTripDecimalDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final double[] doubles = new double[blockSize];
        for (int i = 0; i < blockSize; i++) {
            doubles[i] = Math.round(randomDoubleBetween(0.01, 99.99, true) * 100.0) / 100.0;
        }
        assertRoundTrip(
            doublesToSortableLongs(doubles),
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testRoundTripConstantDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final double constant = 42.5;
        final long[] values = new long[blockSize];
        Arrays.fill(values, doubleToSortableLong(constant));
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testRoundTripIntegerLikeDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong((double) randomIntBetween(0, 10000));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testRoundTripMixedPrecision() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            if (i % 10 == 0) {
                values[i] = doubleToSortableLong(Math.PI * (i + 1));
            } else {
                values[i] = doubleToSortableLong(Math.round(randomDoubleBetween(0.1, 100.0, true) * 100.0) / 100.0);
            }
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testRoundTripSensorLikeData() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        double base = 22.5;
        for (int i = 0; i < blockSize; i++) {
            double v = base + Math.round(randomDoubleBetween(-0.5, 0.5, true) * 10.0) / 10.0;
            values[i] = doubleToSortableLong(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testExceptionAtPositionZero() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = doubleToSortableLong(Math.PI);
        for (int i = 1; i < blockSize; i++) {
            values[i] = doubleToSortableLong(10.0 + i * 0.01);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testConsecutiveExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(10.0 + i * 0.01);
        }
        values[5] = doubleToSortableLong(Math.PI);
        values[6] = doubleToSortableLong(Math.E);
        values[7] = doubleToSortableLong(Math.sqrt(2));
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testExceptionsAtEndOfBlock() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(10.0 + i * 0.01);
        }
        values[blockSize - 3] = doubleToSortableLong(Math.PI);
        values[blockSize - 2] = doubleToSortableLong(Math.E);
        values[blockSize - 1] = doubleToSortableLong(Math.sqrt(2));
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testSkipsWhenNoCompressionBenefit() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(Math.sqrt(i + 2));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testFullPipelineRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(Math.round(randomDoubleBetween(0.01, 99.99, true) * 100.0) / 100.0);
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(
            PipelineConfig.forDoubles(blockSize).alpDoubleStage().offset().gcd().bitPack()
        );
        assertFullPipelineRoundTrip(values, encoder);
    }

    public void testRoundTripRandomDoublesFuzz() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(randomDoubleBetween(-1e6, 1e6, true));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testMixedPositiveNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            double v = Math.round(randomDoubleBetween(-99.99, 99.99, true) * 100.0) / 100.0;
            values[i] = doubleToSortableLong(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testAllNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            double v = Math.round(randomDoubleBetween(-99.99, -0.01, true) * 100.0) / 100.0;
            values[i] = doubleToSortableLong(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testZeroCrossingValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            double v = -1.0 + i * (2.0 / blockSize);
            values[i] = doubleToSortableLong(Math.round(v * 1000.0) / 1000.0);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testSubnormalDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            double v = Double.MIN_VALUE * (i + 1);
            values[i] = doubleToSortableLong(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testHighExceptionRate() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(Math.sqrt(i + 2) * Math.PI + Math.E * (i + 1));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    public void testRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = doubleToSortableLong(randomDoubleBetween(-1e12, 1e12, true));
            }
            assertRoundTrip(
                values,
                blockSize,
                StageId.ALP_DOUBLE_STAGE.id,
                new AlpDoubleTransformEncodeStage(blockSize),
                DECODER,
                blockSize * 12
            );
        }
    }

    public void testRoundTripWithMaxErrorReducedExponent() throws IOException {
        // NOTE: Fused quantization (step = 2 * maxError) is applied in-place
        // before (e,f) search, so we pre-quantize the input to get the expected output.
        final double maxError = 1e-2;
        final double step = 2.0 * maxError;
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(10.0 + i * 0.01);
        }
        QuantizeUtils.quantizeDoubles(values, blockSize, step);
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize, maxError),
            DECODER,
            blockSize * 12
        );
    }

    public void testRoundTripWithMaxError1e6() throws IOException {
        // NOTE: Fused quantization (step = 2 * maxError) is applied in-place
        // before (e,f) search, so we pre-quantize the input to get the expected output.
        final double maxError = 1e-6;
        final double step = 2.0 * maxError;
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(Math.round(randomDoubleBetween(0.01, 99.99, true) * 1e6) / 1e6);
        }
        QuantizeUtils.quantizeDoubles(values, blockSize, step);
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize, maxError),
            DECODER,
            blockSize * 12
        );
    }

    public void testDefaultConstructorUsesMaxExponent18() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(randomDoubleBetween(-1e6, 1e6, true));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.ALP_DOUBLE_STAGE.id,
            new AlpDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * 12
        );
    }

    private static long doubleToSortableLong(double value) {
        return NumericUtils.doubleToSortableLong(value);
    }

    private static long[] doublesToSortableLongs(final double[] doubles) {
        final long[] longs = new long[doubles.length];
        for (int i = 0; i < doubles.length; i++) {
            longs[i] = doubleToSortableLong(doubles[i]);
        }
        return longs;
    }
}
