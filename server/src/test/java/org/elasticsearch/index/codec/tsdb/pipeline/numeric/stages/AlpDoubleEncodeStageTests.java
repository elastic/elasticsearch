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

public class AlpDoubleEncodeStageTests extends PayloadCodecStageTestCase {

    public void testRoundTripDecimalDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final double[] doubles = new double[blockSize];
        for (int i = 0; i < blockSize; i++) {
            doubles[i] = Math.round(randomDoubleBetween(0.01, 99.99, true) * 100.0) / 100.0;
        }
        assertRoundTrip(doublesToSortableLongs(doubles), blockSize);
    }

    public void testRoundTripConstantDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final double constant = 42.5;
        final long[] values = new long[blockSize];
        Arrays.fill(values, doubleToSortableLong(constant));
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripIntegerLikeDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong((double) randomIntBetween(0, 10000));
        }
        assertRoundTrip(values, blockSize);
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
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripSensorLikeData() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        double base = 22.5;
        for (int i = 0; i < blockSize; i++) {
            double v = base + Math.round(randomDoubleBetween(-0.5, 0.5, true) * 10.0) / 10.0;
            values[i] = doubleToSortableLong(v);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripSingleValue() throws IOException {
        assertRoundTrip(new long[] { doubleToSortableLong(3.14) }, randomBlockSize());
    }

    public void testRoundTripNegativeDecimals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(Math.round(randomDoubleBetween(-99.99, -0.01, true) * 100.0) / 100.0);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripSpecialValues() throws IOException {
        long[] values = {
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

    public void testRoundTripMode0x00Fallback() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(Math.sqrt(i + 2));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testExceptionPlaceholderAndRestore() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(10.0 + i * 0.01);
        }
        final int exceptionPos = 42;
        values[exceptionPos] = doubleToSortableLong(Math.PI);
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

    public void testEstimatePrecision() {
        assertEquals(0, AlpDoubleUtils.estimatePrecision(42.0, AlpDoubleUtils.MAX_EXPONENT));
        assertEquals(1, AlpDoubleUtils.estimatePrecision(3.1, AlpDoubleUtils.MAX_EXPONENT));
        assertEquals(2, AlpDoubleUtils.estimatePrecision(3.14, AlpDoubleUtils.MAX_EXPONENT));
        assertEquals(3, AlpDoubleUtils.estimatePrecision(3.141, AlpDoubleUtils.MAX_EXPONENT));
        assertEquals(0, AlpDoubleUtils.estimatePrecision(0.0, AlpDoubleUtils.MAX_EXPONENT));
        assertEquals(0, AlpDoubleUtils.estimatePrecision(Double.NaN, AlpDoubleUtils.MAX_EXPONENT));
        assertEquals(0, AlpDoubleUtils.estimatePrecision(Double.POSITIVE_INFINITY, AlpDoubleUtils.MAX_EXPONENT));
    }

    public void testDeltaEncodedExceptionPositions() throws IOException {
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
            values[i] = doubleToSortableLong(Math.round(randomDoubleBetween(0.01, 99.99, true) * 100.0) / 100.0);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testPartialBlockSingleValue() throws IOException {
        assertRoundTrip(new long[] { doubleToSortableLong(3.14) }, randomBlockSize());
    }

    public void testMixedExceptionsAndDecode() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong(10.0 + i * 0.01);
        }
        values[0] = doubleToSortableLong(Math.PI);
        values[1] = doubleToSortableLong(Math.E);
        values[blockSize / 2] = doubleToSortableLong(Math.sqrt(7));
        values[blockSize - 2] = doubleToSortableLong(Math.log(3));
        values[blockSize - 1] = doubleToSortableLong(Math.cbrt(5));
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripMultipleBlocks() throws IOException {
        final int blockSize = randomBlockSize();
        final AlpDoubleEncodeStage stage = new AlpDoubleEncodeStage(blockSize);

        for (int block = 0; block < 3; block++) {
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = doubleToSortableLong(10.0 + block * 100 + i * 0.01);
            }
            assertRoundTripWithStage(values, blockSize, stage);
        }
    }

    public void testIdentityTransformData() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong((double) (1706000000L + i));
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRandomIntegersAsDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = doubleToSortableLong((double) randomIntBetween(1000, 100000));
        }
        assertRoundTrip(values, blockSize);
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

    private void assertRoundTrip(long[] original, int blockSize) throws IOException {
        assertRoundTripWithStage(original, blockSize, new AlpDoubleEncodeStage(blockSize));
    }

    private void assertRoundTripWithStage(long[] original, int blockSize, AlpDoubleEncodeStage stage) throws IOException {
        final long[] values = new long[blockSize];
        System.arraycopy(original, 0, values, 0, Math.min(original.length, blockSize));

        final EncodingContext encodingContext = createEncodingContext(blockSize, StageId.ALP_DOUBLE.id);
        final byte[] dataBuffer = new byte[blockSize * Long.BYTES + blockSize * 12 + 64];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
        stage.encode(values, original.length, dataOutput, encodingContext);

        final DecodingContext decodingContext = createDecodingContext(blockSize, StageId.ALP_DOUBLE.id);
        final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());

        final long[] decoded = new long[blockSize];
        int decodedCount = new AlpDoubleDecodeStage(blockSize).decode(decoded, dataInput, decodingContext);

        assertEquals(blockSize, decodedCount);
        assertArrayEquals(original, Arrays.copyOf(decoded, original.length));
    }

    private static long doubleToSortableLong(double value) {
        return org.apache.lucene.util.NumericUtils.doubleToSortableLong(value);
    }

    private static long[] doublesToSortableLongs(final double[] doubles) {
        final long[] longs = new long[doubles.length];
        for (int i = 0; i < doubles.length; i++) {
            longs[i] = doubleToSortableLong(doubles[i]);
        }
        return longs;
    }
}
