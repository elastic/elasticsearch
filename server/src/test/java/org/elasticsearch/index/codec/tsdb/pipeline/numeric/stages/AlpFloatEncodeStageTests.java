/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;

import java.io.IOException;
import java.util.Arrays;

public class AlpFloatEncodeStageTests extends PayloadCodecStageTestCase {

    public void testRoundTripDecimalFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(Math.round(randomFloat() * 10000.0f) / 100.0f);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripConstantFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, NumericUtils.floatToSortableInt(42.5f));
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripIntegerLikeFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt((float) randomIntBetween(0, 10000));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripSensorLikeData() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            float v = 22.5f + Math.round(randomFloat() * 10.0f - 5.0f) / 10.0f;
            values[i] = NumericUtils.floatToSortableInt(v);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripSingleValue() throws IOException {
        assertRoundTrip(new long[] { (long) NumericUtils.floatToSortableInt(3.14f) }, randomBlockSize());
    }

    public void testRoundTripNegativeFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(-(Math.round(randomFloat() * 9999.0f) / 100.0f + 0.01f));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripSpecialValues() throws IOException {
        long[] values = {
            (long) NumericUtils.floatToSortableInt(Float.NaN),
            (long) NumericUtils.floatToSortableInt(Float.POSITIVE_INFINITY),
            (long) NumericUtils.floatToSortableInt(Float.NEGATIVE_INFINITY),
            (long) NumericUtils.floatToSortableInt(-0.0f),
            (long) NumericUtils.floatToSortableInt(0.0f),
            (long) NumericUtils.floatToSortableInt(1.5f),
            (long) NumericUtils.floatToSortableInt(42.0f),
            (long) NumericUtils.floatToSortableInt(99.99f) };
        assertRoundTrip(values, randomBlockSize());
    }

    public void testExceptionAtPositionZero() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = NumericUtils.floatToSortableInt((float) Math.PI);
        for (int i = 1; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(10.0f + i * 0.01f);
        }
        assertRoundTrip(values, blockSize);
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
        assertRoundTrip(values, blockSize);
    }

    public void testMixedPositiveNegativeFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(Math.round((randomFloat() * 100.0f - 50.0f) * 100.0f) / 100.0f);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testPartialBlockRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final int partialSize = randomIntBetween(10, blockSize - 1);
        final long[] values = new long[partialSize];
        for (int i = 0; i < partialSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(Math.round(randomFloat() * 10000.0f) / 100.0f);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripMultipleBlocks() throws IOException {
        final int blockSize = randomBlockSize();
        final AlpFloatEncodeStage stage = new AlpFloatEncodeStage(blockSize);

        for (int block = 0; block < 3; block++) {
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = NumericUtils.floatToSortableInt(10.0f + block * 100 + i * 0.01f);
            }
            assertRoundTripWithStage(values, blockSize, stage);
        }
    }

    public void testRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = NumericUtils.floatToSortableInt(randomFloat() * 2e6f - 1e6f);
            }
            assertRoundTrip(values, blockSize);
        }
    }

    private void assertRoundTrip(long[] original, int blockSize) throws IOException {
        assertRoundTripWithStage(original, blockSize, new AlpFloatEncodeStage(blockSize));
    }

    private void assertRoundTripWithStage(long[] original, int blockSize, AlpFloatEncodeStage stage) throws IOException {
        final long[] values = new long[blockSize];
        System.arraycopy(original, 0, values, 0, Math.min(original.length, blockSize));

        final EncodingContext encodingContext = createEncodingContext(blockSize, StageId.ALP_FLOAT.id);
        final byte[] dataBuffer = new byte[blockSize * Long.BYTES + blockSize * 12 + 64];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
        stage.encode(values, original.length, dataOutput, encodingContext);

        final DecodingContext decodingContext = createDecodingContext(blockSize, StageId.ALP_FLOAT.id);
        final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());

        final long[] decoded = new long[blockSize];
        int decodedCount = new AlpFloatDecodeStage(blockSize).decode(decoded, dataInput, decodingContext);

        assertEquals(blockSize, decodedCount);
        assertArrayEquals(original, Arrays.copyOf(decoded, original.length));
    }

}
