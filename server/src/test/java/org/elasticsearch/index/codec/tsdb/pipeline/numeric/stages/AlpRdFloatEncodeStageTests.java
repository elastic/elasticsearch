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

public class AlpRdFloatEncodeStageTests extends PayloadCodecStageTestCase {

    public void testRoundTripDecimalPath() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt(10.0f + i * 0.01f);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripAlpRdPath() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt((float) Math.sqrt(i + 2));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripConstantValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, floatToSortableInt(42.5f));
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripNegativeIrrational() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt((float) -Math.sqrt(i + 2));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripMixedWithExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt(10.0f + i * 0.01f);
        }
        values[0] = floatToSortableInt((float) Math.PI);
        values[blockSize / 3] = floatToSortableInt((float) Math.E);
        values[2 * blockSize / 3] = floatToSortableInt((float) Math.sqrt(2));
        values[blockSize - 1] = floatToSortableInt((float) Math.sqrt(3));
        assertRoundTrip(values, blockSize);
    }

    public void testExceptionAtPositionZero() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = floatToSortableInt((float) Math.PI);
        for (int i = 1; i < blockSize; i++) {
            values[i] = floatToSortableInt(10.0f + i * 0.01f);
        }
        assertRoundTrip(values, blockSize);
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
        assertRoundTrip(values, blockSize);
    }

    public void testExceptionsAtEndOfBlock() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt(10.0f + i * 0.01f);
        }
        values[blockSize - 3] = floatToSortableInt((float) Math.PI);
        values[blockSize - 2] = floatToSortableInt((float) Math.E);
        values[blockSize - 1] = floatToSortableInt((float) Math.sqrt(2));
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripSpecialValues() throws IOException {
        final long[] values = {
            floatToSortableInt(Float.NaN),
            floatToSortableInt(Float.POSITIVE_INFINITY),
            floatToSortableInt(Float.NEGATIVE_INFINITY),
            floatToSortableInt(-0.0f),
            floatToSortableInt(0.0f),
            floatToSortableInt(1.5f),
            floatToSortableInt(42.0f),
            floatToSortableInt(99.99f) };
        assertRoundTrip(values, randomBlockSize());
    }

    public void testRoundTripSingleValue() throws IOException {
        assertRoundTrip(new long[] { floatToSortableInt((float) Math.PI) }, randomBlockSize());
    }

    public void testRoundTripSimilarMagnitudeFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt(1000.0f + i * 0.001f * (float) Math.PI);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testSkipsDecimalModeForIntegerTimestamps() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt((float) (1706000000L + i));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripRandomFloatsFuzz() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt((float) randomDoubleBetween(-1e6, 1e6, true));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testPartialBlockRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final int partialSize = randomIntBetween(10, blockSize - 1);
        final long[] values = new long[partialSize];
        for (int i = 0; i < partialSize; i++) {
            values[i] = floatToSortableInt((float) Math.sqrt(i + 2));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripMultipleBlocks() throws IOException {
        final int blockSize = randomBlockSize();
        final AlpRdFloatEncodeStage stage = new AlpRdFloatEncodeStage(blockSize);

        for (int block = 0; block < 3; block++) {
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = floatToSortableInt(10.0f + block * 100 + i * 0.01f);
            }
            assertRoundTripWithStage(values, blockSize, stage);
        }
    }

    public void testMixedPositiveNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt((float) (Math.round(randomDoubleBetween(-50.0, 50.0, true) * 100.0) / 100.0));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testAllNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt(-100.0f - i * 0.01f);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testZeroCrossingValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt(-0.5f + i * (1.0f / blockSize));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testSubnormalFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt(Float.MIN_VALUE * (i + 1));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testHighExceptionRate() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableInt((float) (Math.sqrt(i + 1) * Math.PI));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = floatToSortableInt((float) randomDoubleBetween(-1e6, 1e6, true));
            }
            assertRoundTrip(values, blockSize);
        }
    }

    private void assertRoundTrip(final long[] original, int blockSize) throws IOException {
        assertRoundTripWithStage(original, blockSize, new AlpRdFloatEncodeStage(blockSize));
    }

    private void assertRoundTripWithStage(long[] original, int blockSize, AlpRdFloatEncodeStage stage) throws IOException {
        final long[] values = new long[blockSize];
        System.arraycopy(original, 0, values, 0, Math.min(original.length, blockSize));

        final EncodingContext encodingContext = createEncodingContext(blockSize, StageId.ALP_RD_FLOAT.id);
        final byte[] dataBuffer = new byte[blockSize * Long.BYTES + blockSize * 12 + 64];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
        stage.encode(values, original.length, dataOutput, encodingContext);

        final DecodingContext decodingContext = createDecodingContext(blockSize, StageId.ALP_RD_FLOAT.id);
        final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());

        final long[] decoded = new long[blockSize];
        int decodedCount = new AlpRdFloatDecodeStage(blockSize).decode(decoded, dataInput, decodingContext);

        assertEquals(blockSize, decodedCount);
        assertArrayEquals(original, Arrays.copyOf(decoded, original.length));
    }

    private static long floatToSortableInt(float value) {
        return NumericUtils.floatToSortableInt(value);
    }
}
