/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.LongStream;

public class RleEncodeStageTests extends NumericCodecStageTestCase {

    private static final TransformDecoder DECODER = new RleDecodeStage();

    public void testRoundTripAllSameValue() throws IOException {
        final int blockSize = randomBlockSize();
        final long value = randomLong();
        assertRoundTrip(LongStream.generate(() -> value).limit(blockSize).toArray(), blockSize);
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
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripAlternatingRuns() throws IOException {
        // NOTE: alternating pairs [A,A,B,B,A,A,B,B,...] produce moderate compression.
        final int blockSize = randomBlockSize();
        final long a = randomLong();
        final long b = randomLong();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = ((i / 2) % 2 == 0) ? a : b;
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripNegativeValues() throws IOException {
        // NOTE: negative sortable-longs exercise sign-bit handling in metadata.
        final int blockSize = randomBlockSize();
        final long neg = Long.MIN_VALUE + randomIntBetween(0, 1000);
        final long[] values = new long[blockSize];
        Arrays.fill(values, neg);
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripSingleElement() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[] { randomLong() }, blockSize);
    }

    public void testSkipAllDistinct() throws IOException {
        // NOTE: all distinct values means runCount == valueCount, ratio = 1.0 > 0.90.
        final int blockSize = randomBlockSize();
        final long[] values = LongStream.range(0, blockSize).toArray();
        assertStageSkipped(values, blockSize, StageId.RLE.id, new RleEncodeStage(blockSize));
    }

    public void testSkipStrictlyIncreasing() throws IOException {
        // NOTE: strictly increasing sequence -- runCount == valueCount, ratio = 1.0 > 0.90.
        final int blockSize = randomBlockSize();
        final long base = randomLongBetween(0, Long.MAX_VALUE / 2);
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = base + i;
        }
        assertStageSkipped(values, blockSize, StageId.RLE.id, new RleEncodeStage(blockSize));
    }

    public void testFuzzRandomizedSkip() throws IOException {
        // NOTE: all random longs -- with 2^64 range, collisions are negligible, so ratio ~ 1.0.
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = randomLong();
        }
        assertStageSkipped(values, blockSize, StageId.RLE.id, new RleEncodeStage(blockSize));
    }

    public void testFuzzRandomizedRoundTrip() throws IOException {
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
        // NOTE: may be applied or skipped depending on random data; round-trip still valid.
        final int extraBuffer = blockSize * 5 + 64;
        assertRoundTrip(values, blockSize, StageId.RLE.id, new RleEncodeStage(blockSize), DECODER, extraBuffer);
    }

    public void testConstructorValidation() {
        expectThrows(IllegalArgumentException.class, () -> new RleEncodeStage(128, 0.0));
        expectThrows(IllegalArgumentException.class, () -> new RleEncodeStage(128, -0.1));
        expectThrows(IllegalArgumentException.class, () -> new RleEncodeStage(128, 1.1));
    }

    private void assertRoundTrip(final long[] original, final int blockSize) throws IOException {
        // NOTE: RLE metadata can be large (runCount VInts + runCount header), so use a generous extra buffer.
        // Worst case: blockSize runs * 5 bytes/VInt + 5 bytes for runCount header.
        final int extraBuffer = blockSize * 5 + 64;
        assertRoundTrip(original, blockSize, StageId.RLE.id, new RleEncodeStage(blockSize), DECODER, extraBuffer);
    }
}
