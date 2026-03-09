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
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;

import java.io.IOException;
import java.util.Arrays;

public class AlpRdDoubleEncodeStageTests extends PayloadCodecStageTestCase {

    public void testRoundTripDecimalPath() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(10.0 + i * 0.01);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripAlpRdPath() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(Math.sqrt(i + 2));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripConstantValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, doubleToSortableLong(42.5));
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripNegativeIrrational() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(-Math.sqrt(i + 2));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripMixedWithExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(10.0 + i * 0.01);
        }
        values[0] = doubleToSortableLong(Math.PI);
        values[blockSize / 3] = doubleToSortableLong(Math.E);
        values[2 * blockSize / 3] = doubleToSortableLong(Math.sqrt(2));
        values[blockSize - 1] = doubleToSortableLong(Math.sqrt(3));
        assertRoundTrip(values, blockSize);
    }

    public void testExceptionAtPositionZero() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        values[0] = doubleToSortableLong(Math.PI);
        for (int i = 1; i < blockSize; i++) {
            values[i] = doubleToSortableLong(10.0 + i * 0.01);
        }
        assertRoundTrip(values, blockSize);
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
        assertRoundTrip(values, blockSize);
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
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripSpecialValues() throws IOException {
        final long[] values = {
            doubleToSortableLong(Double.NaN),
            doubleToSortableLong(Double.POSITIVE_INFINITY),
            doubleToSortableLong(Double.NEGATIVE_INFINITY),
            doubleToSortableLong(-0.0),
            doubleToSortableLong(0.0),
            doubleToSortableLong(1.5),
            doubleToSortableLong(42.0),
            doubleToSortableLong(99.99) };
        assertRoundTrip(values, randomBlockSize());
    }

    public void testRoundTripSingleValue() throws IOException {
        assertRoundTrip(new long[] { doubleToSortableLong(Math.PI) }, randomBlockSize());
    }

    public void testRoundTripSimilarMagnitudeDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(1000.0 + i * 0.001 * Math.PI);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testSkipsDecimalModeForIntegerTimestamps() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong((double) (1706000000L + i));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripRandomDoublesFuzz() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(randomDoubleBetween(-1e6, 1e6, true));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testPartialBlockRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final int partialSize = randomIntBetween(10, blockSize - 1);
        final long[] values = new long[partialSize];
        for (int i = 0; i < partialSize; i++) {
            values[i] = doubleToSortableLong(Math.sqrt(i + 2));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripMultipleBlocks() throws IOException {
        final int blockSize = randomBlockSize();
        final AlpRdDoubleEncodeStage stage = new AlpRdDoubleEncodeStage(blockSize);

        for (int block = 0; block < 3; block++) {
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = doubleToSortableLong(10.0 + block * 100 + i * 0.01);
            }
            assertRoundTripWithStage(values, blockSize, stage);
        }
    }

    public void testMixedPositiveNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(Math.round(randomDoubleBetween(-50.0, 50.0, true) * 100.0) / 100.0);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testAllNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(-100.0 - i * 0.01);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testZeroCrossingValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(-0.5 + i * (1.0 / blockSize));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testSubnormalDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(Double.MIN_VALUE * (i + 1));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testHighExceptionRate() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(Math.sqrt(i + 1) * Math.PI);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = doubleToSortableLong(randomDoubleBetween(-1e10, 1e10, true));
            }
            assertRoundTrip(values, blockSize);
        }
    }

    private void assertRoundTrip(final long[] original, int blockSize) throws IOException {
        assertRoundTripWithStage(original, blockSize, new AlpRdDoubleEncodeStage(blockSize));
    }

    private void assertRoundTripWithStage(long[] original, int blockSize, AlpRdDoubleEncodeStage stage) throws IOException {
        final long[] values = new long[blockSize];
        System.arraycopy(original, 0, values, 0, Math.min(original.length, blockSize));

        final EncodingContext encodingContext = createEncodingContext(blockSize, StageId.ALP_RD_DOUBLE.id);
        final byte[] dataBuffer = new byte[blockSize * Long.BYTES + blockSize * 12 + 64];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
        stage.encode(values, original.length, dataOutput, encodingContext);

        final DecodingContext decodingContext = createDecodingContext(blockSize, StageId.ALP_RD_DOUBLE.id);
        final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());

        final long[] decoded = new long[blockSize];
        int decodedCount = new AlpRdDoubleDecodeStage(blockSize).decode(decoded, dataInput, decodingContext);

        assertEquals(blockSize, decodedCount);
        assertArrayEquals(original, Arrays.copyOf(decoded, original.length));
    }

    private static long doubleToSortableLong(double value) {
        return org.apache.lucene.util.NumericUtils.doubleToSortableLong(value);
    }
}
