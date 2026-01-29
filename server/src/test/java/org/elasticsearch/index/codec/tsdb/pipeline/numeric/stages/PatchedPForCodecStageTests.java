/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;

import java.io.IOException;
import java.util.stream.LongStream;

public class PatchedPForCodecStageTests extends NumericCodecStageTestCase {

    private static final int PATCHED_PFOR_EXTRA_BUFFER = 1024;

    public void testIdAndName() {
        assertEquals((byte) 0x04, new PatchedPForCodecStage().id());
        assertEquals("patched-pfor", new PatchedPForCodecStage().name());
    }

    public void testInvalidMaxExceptionPercentThrows() {
        expectThrows(AssertionError.class, () -> new PatchedPForCodecStage(0));
        expectThrows(AssertionError.class, () -> new PatchedPForCodecStage(-1));
        expectThrows(AssertionError.class, () -> new PatchedPForCodecStage(51));
        expectThrows(AssertionError.class, () -> new PatchedPForCodecStage(100));
    }

    public void testRoundTripSingleException() throws IOException {
        final int blockSize = randomBlockSize();
        final long outlier = randomLongBetween(100000, 1000000);
        assertRoundTrip(
            LongStream.concat(LongStream.generate(() -> randomLongBetween(1, 10)).limit(blockSize - 1), LongStream.of(outlier)).toArray(),
            blockSize
        );
    }

    public void testRoundTripMultipleExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = LongStream.generate(() -> randomLongBetween(1, 10)).limit(blockSize).toArray();
        for (int i = 0; i < 3; i++) {
            values[randomIntBetween(0, blockSize - 1)] = randomLongBetween(100000, 1000000);
        }
        assertRoundTrip(values, blockSize, 40);
    }

    public void testRoundTripExceptionAtStart() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = LongStream.generate(() -> randomLongBetween(1, 10)).limit(blockSize).toArray();
        values[0] = randomLongBetween(100000, 1000000);
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripExceptionAtEnd() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = LongStream.generate(() -> randomLongBetween(1, 10)).limit(blockSize).toArray();
        values[blockSize - 1] = randomLongBetween(100000, 1000000);
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripLargeOutliers() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = LongStream.generate(() -> randomLongBetween(1, 10)).limit(blockSize).toArray();
        values[randomIntBetween(0, blockSize - 1)] = Long.MAX_VALUE / 2;
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripUniformValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long value = randomLongBetween(1, 100);
        assertRoundTrip(LongStream.generate(() -> value).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripAllZeros() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[blockSize], blockSize);
    }

    public void testRoundTripManyExceptions() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.generate(() -> randomLongBetween(100000, 1000000)).limit(blockSize).toArray(), blockSize, 1);
    }

    public void testNegativeValuesSkip() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = LongStream.generate(() -> randomLongBetween(-1000, 1000)).limit(blockSize).toArray();
        for (int i = 0; i < Math.min(3, blockSize); i++) {
            values[randomIntBetween(0, blockSize - 1)] = -randomIntBetween(1, 1000);
        }
        final long[] original = values.clone();
        final EncodingContext encodingContext = createEncodingContext(blockSize, StageId.PATCHED_PFOR.id);
        PatchedPForCodecStage.INSTANCE.encode(values, values.length, encodingContext);
        assertArrayEquals(original, values);
        assertFalse(encodingContext.isStageApplied(0));
    }

    private void assertRoundTrip(final long[] original, int blockSize) throws IOException {
        assertRoundTrip(original, blockSize, 10);
    }

    private void assertRoundTrip(final long[] original, int blockSize, int maxExceptionPercent) throws IOException {
        assertRoundTrip(
            original,
            blockSize,
            StageId.PATCHED_PFOR.id,
            new PatchedPForCodecStage(maxExceptionPercent),
            PATCHED_PFOR_EXTRA_BUFFER
        );
    }
}
