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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class ChimpDoubleEncodeStageTests extends ESTestCase {

    private static final int BLOCK_SIZE = 128;

    public void testRandomDoubles() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(0.0, 100.0, true));
        }
        assertRoundTrip(data);
    }

    public void testSlowlyChangingDoubles() throws IOException {
        final double[] doubles = new double[BLOCK_SIZE];
        doubles[0] = randomDoubleBetween(20.0, 30.0, true);
        for (int i = 1; i < BLOCK_SIZE; i++) {
            doubles[i] = doubles[i - 1] + randomDoubleBetween(-0.1, 0.1, true);
        }
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.doubleToSortableLong(doubles[i]);
        }
        assertRoundTrip(data);
    }

    public void testConstantDoubles() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        final long sortableVal = NumericUtils.doubleToSortableLong(3.14);
        Arrays.fill(data, sortableVal);
        assertRoundTrip(data);
    }

    public void testSpecialValues() throws IOException {
        final long[] data = new long[] {
            NumericUtils.doubleToSortableLong(0.0),
            NumericUtils.doubleToSortableLong(-0.0),
            NumericUtils.doubleToSortableLong(Double.MIN_VALUE),
            NumericUtils.doubleToSortableLong(Double.MAX_VALUE),
            NumericUtils.doubleToSortableLong(Double.NEGATIVE_INFINITY),
            NumericUtils.doubleToSortableLong(Double.POSITIVE_INFINITY),
            NumericUtils.doubleToSortableLong(Double.NaN),
            NumericUtils.doubleToSortableLong(1.0) };
        assertRoundTrip(data);
    }

    public void testEmptyArray() throws IOException {
        assertRoundTrip(new long[0]);
    }

    public void testSingleDouble() throws IOException {
        assertRoundTrip(new long[] { NumericUtils.doubleToSortableLong(42.5) });
    }

    public void testTwoDoubles() throws IOException {
        assertRoundTrip(new long[] { NumericUtils.doubleToSortableLong(1.0), NumericUtils.doubleToSortableLong(2.0) });
    }

    public void testAlternatingValues() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.doubleToSortableLong(i % 2 == 0 ? 1.0 : 2.0);
        }
        assertRoundTrip(data);
    }

    public void testSignFlipXor() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.doubleToSortableLong(i % 2 == 0 ? 1.0 : -1.0);
        }
        assertRoundTrip(data);
    }

    public void testSubnormals() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.doubleToSortableLong(Double.MIN_VALUE * (i + 1));
        }
        assertRoundTrip(data);
    }

    public void testNegativeDoubles() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.doubleToSortableLong(-1000.0 + i * 0.1);
        }
        assertRoundTrip(data);
    }

    public void testLargeLeadingZerosXor() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        final double base = 1000.0;
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.doubleToSortableLong(base + i * 1e-12);
        }
        assertRoundTrip(data);
    }

    public void testLargeLeadingZerosCompression() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        final double base = 1000.0;
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.doubleToSortableLong(base + i * 1e-12);
        }
        final byte[] buffer = new byte[data.length * 16 + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

        final EncodingContext encContext = new EncodingContext(BLOCK_SIZE, 1);
        new ChimpDoubleEncodeStage().encode(data.clone(), data.length, out, encContext);

        assertTrue("encoded size " + out.getPosition() + " should be less than raw 1024", out.getPosition() < 768);
    }

    public void testAllZeroXor() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        final long val = NumericUtils.doubleToSortableLong(42.123456789);
        Arrays.fill(data, val);
        assertRoundTrip(data);

        final byte[] buffer = new byte[data.length * 16 + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        final EncodingContext encContext = new EncodingContext(BLOCK_SIZE, 1);
        new ChimpDoubleEncodeStage().encode(data.clone(), data.length, out, encContext);

        assertTrue("all-zero XOR should encode very compactly, got " + out.getPosition(), out.getPosition() <= 30);
    }

    public void testFuzzRandomBlockSize() throws IOException {
        final int blockSize = randomIntBetween(1, 512);
        final long[] data = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            data[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(-1e6, 1e6, true));
        }
        assertRoundTrip(data, blockSize);
    }

    public void testWindowReuse() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.doubleToSortableLong(100.0 + i);
        }
        assertRoundTrip(data);
    }

    public void testMaxXorTwoValues() throws IOException {
        final long[] data = new long[] {
            NumericUtils.doubleToSortableLong(Double.longBitsToDouble(0L)),
            NumericUtils.doubleToSortableLong(Double.longBitsToDouble(0x7FFFFFFFFFFFFFFFL)) };
        assertRoundTrip(data);
    }

    public void testLargeBlockFuzz() throws IOException {
        final int blockSize = 512;
        final long[] data = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            data[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(-1e15, 1e15, true));
        }
        assertRoundTrip(data, blockSize);
    }

    public void testMixedZeroAndNonZeroXor() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        double v = 1.0;
        for (int i = 0; i < BLOCK_SIZE; i++) {
            if (i % 10 == 0) {
                v = randomDoubleBetween(-100.0, 100.0, true);
            }
            data[i] = NumericUtils.doubleToSortableLong(v);
        }
        assertRoundTrip(data);
    }

    public void testRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 100; iter++) {
            final int blockSize = randomIntBetween(2, 256);
            final long[] data = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                data[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(-1e12, 1e12, true));
            }
            assertRoundTrip(data, blockSize);
        }
    }

    public void testStageId() {
        assertThat(new ChimpDoubleEncodeStage().id(), equalTo(StageId.CHIMP_DOUBLE_PAYLOAD.id));
        assertThat(new ChimpDoubleDecodeStage().id(), equalTo(StageId.CHIMP_DOUBLE_PAYLOAD.id));
    }

    private void assertRoundTrip(final long[] original) throws IOException {
        assertRoundTrip(original, BLOCK_SIZE);
    }

    private void assertRoundTrip(final long[] original, final int blockSize) throws IOException {
        final byte[] buffer = new byte[original.length * 16 + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

        final EncodingContext encContext = new EncodingContext(blockSize, 1);
        final DecodingContext decContext = new DecodingContext(blockSize, 1);

        new ChimpDoubleEncodeStage().encode(original.clone(), original.length, out, encContext);

        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        final long[] decoded = new long[Math.max(blockSize, original.length)];
        final int decodedCount = new ChimpDoubleDecodeStage().decode(decoded, in, decContext);

        assertThat("Value count mismatch", decodedCount, equalTo(original.length));
        for (int i = 0; i < original.length; i++) {
            assertThat("Value mismatch at index " + i, decoded[i], equalTo(original[i]));
        }
    }
}
