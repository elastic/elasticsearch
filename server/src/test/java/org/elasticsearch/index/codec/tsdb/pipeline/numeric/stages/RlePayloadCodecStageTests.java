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
import java.util.stream.LongStream;

public class RlePayloadCodecStageTests extends PayloadCodecStageTestCase {

    public void testRoundTripAllSame() throws IOException {
        final int blockSize = randomBlockSize();
        final long value = randomLong();
        assertRoundTrip(LongStream.generate(() -> value).limit(blockSize).toArray(), blockSize, blockSize);
    }

    public void testRoundTripAllDistinct() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.range(0, blockSize).toArray(), blockSize, blockSize);
    }

    public void testRoundTripNegativeValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long neg = Long.MIN_VALUE + randomIntBetween(0, 1000);
        assertRoundTrip(LongStream.generate(() -> neg).limit(blockSize).toArray(), blockSize, blockSize);
    }

    public void testRoundTripTwoRuns() throws IOException {
        final int blockSize = randomBlockSize();
        final int half = blockSize / 2;
        final long a = randomLong();
        final long b = randomLong();
        final long[] values = new long[blockSize];
        for (int i = 0; i < half; i++) {
            values[i] = a;
        }
        for (int i = half; i < blockSize; i++) {
            values[i] = b;
        }
        assertRoundTrip(values, blockSize, blockSize);
    }

    public void testRoundTripSingleValue() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[] { randomLong() }, 1, blockSize);
    }

    public void testRoundTripEmpty() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[0], 0, blockSize);
    }

    public void testFuzzRandomized() throws IOException {
        // NOTE: randomized fuzz with a mix of runs and distinct values.
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        long current = randomLong();
        for (int i = 0; i < blockSize; i++) {
            if (randomBoolean() && randomBoolean()) {
                current = randomLong();
            }
            values[i] = current;
        }
        assertRoundTrip(values, blockSize, blockSize);
    }

    public void testFuzz64BitValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(
            LongStream.concat(LongStream.of(Long.MAX_VALUE, Long.MIN_VALUE), LongStream.generate(RlePayloadCodecStageTests::randomLong))
                .limit(blockSize)
                .toArray(),
            blockSize,
            blockSize
        );
    }

    private void assertRoundTrip(final long[] original, final int valueCount, final int blockSize) throws IOException {
        final long[] values = new long[Math.max(blockSize, valueCount)];
        System.arraycopy(original, 0, values, 0, Math.min(original.length, valueCount));

        final EncodingContext encodingContext = createEncodingContext(blockSize, StageId.RLE_PAYLOAD.id);

        // NOTE: generous buffer -- worst case is all distinct (8 bytes per value + VInt overhead).
        final byte[] dataBuffer = new byte[blockSize * (Long.BYTES + 5) + 64];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
        RlePayloadCodecStage.INSTANCE.encode(values, valueCount, dataOutput, encodingContext);

        final DecodingContext decodingContext = createDecodingContext(blockSize, StageId.RLE_PAYLOAD.id);
        final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());

        final long[] decoded = new long[Math.max(blockSize, valueCount)];
        final int decodedCount = RlePayloadCodecStage.INSTANCE.decode(decoded, dataInput, decodingContext);

        assertEquals(valueCount, decodedCount);
        assertArrayEquals(original, Arrays.copyOf(decoded, original.length));
    }
}
