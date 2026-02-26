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
import static org.hamcrest.Matchers.lessThan;

public class Chimp128FloatEncodeStageTests extends ESTestCase {

    private static final int BLOCK_SIZE = 128;

    public void testRandomFloats() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            final float f = (float) randomDoubleBetween(0.0, 100.0, true);
            data[i] = NumericUtils.floatToSortableInt(f);
        }
        assertRoundTrip(data);
    }

    public void testSlowlyChangingFloats() throws IOException {
        final float[] floats = new float[BLOCK_SIZE];
        floats[0] = (float) randomDoubleBetween(20.0, 30.0, true);
        for (int i = 1; i < BLOCK_SIZE; i++) {
            floats[i] = floats[i - 1] + (float) randomDoubleBetween(-0.1, 0.1, true);
        }
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.floatToSortableInt(floats[i]);
        }
        assertRoundTrip(data);
    }

    public void testConstantFloats() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        final long sortableVal = NumericUtils.floatToSortableInt(3.14f);
        Arrays.fill(data, sortableVal);
        assertRoundTrip(data);
    }

    public void testEmptyArray() throws IOException {
        assertRoundTrip(new long[0]);
    }

    public void testSingleFloat() throws IOException {
        assertRoundTrip(new long[] { NumericUtils.floatToSortableInt(42.5f) });
    }

    public void testTwoFloats() throws IOException {
        assertRoundTrip(new long[] { NumericUtils.floatToSortableInt(1.0f), NumericUtils.floatToSortableInt(2.0f) });
    }

    public void testAlternatingValues() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.floatToSortableInt(i % 2 == 0 ? 1.0f : 2.0f);
        }
        assertRoundTrip(data);
    }

    public void testSignFlipXor() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.floatToSortableInt(i % 2 == 0 ? 1.0f : -1.0f);
        }
        assertRoundTrip(data);
    }

    public void testNegativeFloats() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.floatToSortableInt(-1000.0f + i * 0.1f);
        }
        assertRoundTrip(data);
    }

    public void testSpecialValues() throws IOException {
        final long[] data = new long[] {
            NumericUtils.floatToSortableInt(0.0f),
            NumericUtils.floatToSortableInt(-0.0f),
            NumericUtils.floatToSortableInt(Float.MIN_VALUE),
            NumericUtils.floatToSortableInt(Float.MAX_VALUE),
            NumericUtils.floatToSortableInt(Float.NEGATIVE_INFINITY),
            NumericUtils.floatToSortableInt(Float.POSITIVE_INFINITY),
            NumericUtils.floatToSortableInt(Float.NaN),
            NumericUtils.floatToSortableInt(1.0f) };
        assertRoundTrip(data);
    }

    public void testAllZeroXorCompression() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        final long val = NumericUtils.floatToSortableInt(42.123f);
        Arrays.fill(data, val);

        final byte[] buffer = new byte[data.length * 8 + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        final EncodingContext encContext = new EncodingContext(BLOCK_SIZE, 1);
        new Chimp128FloatEncodeStage().encode(data.clone(), data.length, out, encContext);

        // NOTE: Chimp128 writes (1 flag bit + indexBits) per zero-XOR entry, so constant data
        // is larger than plain Chimp but still well below the raw 512-byte uncompressed size.
        assertTrue("all-zero XOR should encode compactly, got " + out.getPosition(), out.getPosition() <= 140);
    }

    public void testFuzzRandomBlockSize() throws IOException {
        final int blockSize = randomIntBetween(1, 512);
        final long[] data = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            data[i] = NumericUtils.floatToSortableInt((float) randomDoubleBetween(-1e6, 1e6, true));
        }
        assertRoundTrip(data, blockSize);
    }

    public void testRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 100; iter++) {
            final int blockSize = randomIntBetween(2, 256);
            final long[] data = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                data[i] = NumericUtils.floatToSortableInt((float) randomDoubleBetween(-1e6, 1e6, true));
            }
            assertRoundTrip(data, blockSize);
        }
    }

    public void testStageId() {
        assertThat(new Chimp128FloatEncodeStage().id(), equalTo(StageId.CHIMP128_FLOAT_PAYLOAD.id));
        assertThat(new Chimp128FloatDecodeStage().id(), equalTo(StageId.CHIMP128_FLOAT_PAYLOAD.id));
    }

    public void testPeriodicPattern() throws IOException {
        final float[] pattern = { 10.0f, 20.0f, 30.0f, 40.0f };
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.floatToSortableInt(pattern[i % pattern.length]);
        }
        assertRoundTrip(data);
    }

    public void testCustomBufferSize() throws IOException {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.floatToSortableInt((float) randomDoubleBetween(-100.0, 100.0, true));
        }
        for (int bufSize : new int[] { 8, 32, 64, 256 }) {
            final byte[] buffer = new byte[data.length * 8 + 256];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
            final EncodingContext encContext = new EncodingContext(BLOCK_SIZE, 1);
            final DecodingContext decContext = new DecodingContext(BLOCK_SIZE, 1);

            new Chimp128FloatEncodeStage(bufSize).encode(data.clone(), data.length, out, encContext);

            final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
            final long[] decoded = new long[BLOCK_SIZE];
            final int decodedCount = new Chimp128FloatDecodeStage().decode(decoded, in, decContext);

            assertThat("Value count mismatch for bufferSize=" + bufSize, decodedCount, equalTo(data.length));
            for (int i = 0; i < data.length; i++) {
                assertThat("Value mismatch at index " + i + " for bufferSize=" + bufSize, decoded[i], equalTo(data[i]));
            }
        }
    }

    public void testChimp128BetterThanChimpOnPeriodic() throws IOException {
        final float[] pattern = { 10.0f, 20.0f, 30.0f, 40.0f };
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = NumericUtils.floatToSortableInt(pattern[i % pattern.length]);
        }

        final byte[] chimpBuffer = new byte[data.length * 8 + 256];
        final ByteArrayDataOutput chimpOut = new ByteArrayDataOutput(chimpBuffer);
        final EncodingContext chimpEncContext = new EncodingContext(BLOCK_SIZE, 1);
        new ChimpFloatEncodeStage().encode(data.clone(), data.length, chimpOut, chimpEncContext);
        final int chimpBytes = chimpOut.getPosition();

        // NOTE: Use bufferSize=4 to match the period length; this minimizes index overhead
        // (2 bits) while enabling exact ring buffer matches for all repeated values.
        final byte[] chimp128Buffer = new byte[data.length * 8 + 256];
        final ByteArrayDataOutput chimp128Out = new ByteArrayDataOutput(chimp128Buffer);
        final EncodingContext chimp128EncContext = new EncodingContext(BLOCK_SIZE, 1);
        new Chimp128FloatEncodeStage(4).encode(data.clone(), data.length, chimp128Out, chimp128EncContext);
        final int chimp128Bytes = chimp128Out.getPosition();

        assertThat(
            "Chimp128 (" + chimp128Bytes + " bytes) should compress periodic data better than Chimp (" + chimpBytes + " bytes)",
            chimp128Bytes,
            lessThan(chimpBytes)
        );
    }

    private void assertRoundTrip(final long[] original) throws IOException {
        assertRoundTrip(original, BLOCK_SIZE);
    }

    private void assertRoundTrip(final long[] original, final int blockSize) throws IOException {
        final byte[] buffer = new byte[original.length * 8 + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

        final EncodingContext encContext = new EncodingContext(blockSize, 1);
        final DecodingContext decContext = new DecodingContext(blockSize, 1);

        new Chimp128FloatEncodeStage().encode(original.clone(), original.length, out, encContext);

        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        final long[] decoded = new long[Math.max(blockSize, original.length)];
        final int decodedCount = new Chimp128FloatDecodeStage().decode(decoded, in, decContext);

        assertThat("Value count mismatch", decodedCount, equalTo(original.length));
        for (int i = 0; i < original.length; i++) {
            assertThat("Value mismatch at index " + i, decoded[i], equalTo(original[i]));
        }
    }
}
