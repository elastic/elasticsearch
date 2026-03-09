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

public class AlpFloatTransformEncodeStageTests extends NumericCodecStageTestCase {

    private static final TransformDecoder DECODER = new AlpFloatTransformDecodeStage();

    public void testRoundTripDecimalFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt(Math.round(randomFloat() * 10000.0f) / 100.0f);
        }
        assertRoundTrip(values, blockSize, StageId.ALP_FLOAT_STAGE.id, new AlpFloatTransformEncodeStage(blockSize), DECODER, blockSize * 8);
    }

    public void testRoundTripConstantFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, floatToSortableInt(42.5f));
        assertRoundTrip(values, blockSize, StageId.ALP_FLOAT_STAGE.id, new AlpFloatTransformEncodeStage(blockSize), DECODER, blockSize * 8);
    }

    public void testRoundTripIntegerLikeFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt((float) randomIntBetween(0, 10000));
        }
        assertRoundTrip(values, blockSize, StageId.ALP_FLOAT_STAGE.id, new AlpFloatTransformEncodeStage(blockSize), DECODER, blockSize * 8);
    }

    public void testRoundTripSensorLikeData() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            float v = 22.5f + Math.round(randomFloat() * 10.0f - 5.0f) / 10.0f;
            values[i] = floatToSortableInt(v);
        }
        assertRoundTrip(values, blockSize, StageId.ALP_FLOAT_STAGE.id, new AlpFloatTransformEncodeStage(blockSize), DECODER, blockSize * 8);
    }

    public void testExceptionAtPositionZero() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = floatToSortableInt((float) Math.PI);
        for (int i = 1; i < blockSize; i++) {
            values[i] = floatToSortableInt(10.0f + i * 0.01f);
        }
        assertRoundTrip(values, blockSize, StageId.ALP_FLOAT_STAGE.id, new AlpFloatTransformEncodeStage(blockSize), DECODER, blockSize * 8);
    }

    public void testConsecutiveExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt(10.0f + i * 0.01f);
        }
        values[5] = floatToSortableInt((float) Math.PI);
        values[6] = floatToSortableInt((float) Math.E);
        values[7] = floatToSortableInt((float) Math.sqrt(2));
        assertRoundTrip(values, blockSize, StageId.ALP_FLOAT_STAGE.id, new AlpFloatTransformEncodeStage(blockSize), DECODER, blockSize * 8);
    }

    public void testMixedPositiveNegativeFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            float v = Math.round((randomFloat() * 200.0f - 100.0f) * 100.0f) / 100.0f;
            values[i] = floatToSortableInt(v);
        }
        assertRoundTrip(values, blockSize, StageId.ALP_FLOAT_STAGE.id, new AlpFloatTransformEncodeStage(blockSize), DECODER, blockSize * 8);
    }

    public void testAllNegativeFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            float v = -(Math.round(randomFloat() * 9999.0f) / 100.0f + 0.01f);
            values[i] = floatToSortableInt(v);
        }
        assertRoundTrip(values, blockSize, StageId.ALP_FLOAT_STAGE.id, new AlpFloatTransformEncodeStage(blockSize), DECODER, blockSize * 8);
    }

    public void testRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = floatToSortableInt(randomFloat() * 2e6f - 1e6f);
            }
            assertRoundTrip(
                values,
                blockSize,
                StageId.ALP_FLOAT_STAGE.id,
                new AlpFloatTransformEncodeStage(blockSize),
                DECODER,
                blockSize * 8
            );
        }
    }

    private static long floatToSortableInt(float value) {
        return NumericUtils.floatToSortableInt(value);
    }
}
