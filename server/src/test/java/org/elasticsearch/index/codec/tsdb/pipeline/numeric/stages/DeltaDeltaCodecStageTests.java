/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.IOException;
import java.util.stream.LongStream;

public class DeltaDeltaCodecStageTests extends NumericCodecStageTestCase {

    public void testRoundTripConstantInterval() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = randomLongBetween(1_000_000_000_000L, 2_000_000_000_000L);
        final long interval = randomLongBetween(1000, 60_000);
        assertRoundTrip(LongStream.iterate(start, v -> v + interval).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripConstantIntervalWithJitter() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = randomLongBetween(1_000_000_000_000L, 2_000_000_000_000L);
        final long interval = 10_000;
        long[] values = new long[blockSize];
        long v = start;
        for (int i = 0; i < blockSize; i++) {
            values[i] = v;
            v += interval + randomLongBetween(-100, 100);
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripMonotonicIncreasing() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = randomLongBetween(0, 1000);
        final long increment = randomLongBetween(1, 100);
        assertRoundTrip(LongStream.iterate(start, v -> v + increment).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripMonotonicDecreasing() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = randomLongBetween(100_000, 200_000);
        final long decrement = randomLongBetween(1, 100);
        assertRoundTrip(LongStream.iterate(start, v -> v - decrement).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripAllSame() throws IOException {
        final int blockSize = randomBlockSize();
        final long value = randomLong();
        assertRoundTrip(LongStream.generate(() -> value).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripTwoValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[] { randomLongBetween(0, 100), randomLongBetween(101, 200) }, blockSize);
    }

    public void testRoundTripSingleValue() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[] { randomLong() }, blockSize);
    }

    public void testRoundTripNonMonotonicData() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.generate(() -> randomLongBetween(-1000, 1000)).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripWithNegativeValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = randomLongBetween(-10_000, -1_000);
        final long increment = randomLongBetween(1, 50);
        assertRoundTrip(LongStream.iterate(start, v -> v + increment).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripLargeValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = Long.MAX_VALUE - blockSize * 100L;
        assertRoundTrip(LongStream.iterate(start, v -> v + randomLongBetween(1, 50)).limit(blockSize).toArray(), blockSize);
    }

    public void testRoundTripWithLargeGap() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = 1_000_000_000_000L;
        final long interval = 10_000;
        long[] values = new long[blockSize];
        long v = start;
        for (int i = 0; i < blockSize; i++) {
            values[i] = v;
            if (i == blockSize / 2) {
                v += interval * 100;
            } else {
                v += interval;
            }
        }
        assertRoundTrip(values, blockSize);
    }

    public void testRoundTripFuzzyTimestamps() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long start = randomLongBetween(0, Long.MAX_VALUE / 2);
            final long baseInterval = randomLongBetween(1, 1_000_000);
            final long maxJitter = Math.max(1, baseInterval / randomIntBetween(2, 100));
            long[] values = new long[blockSize];
            long v = start;
            for (int i = 0; i < blockSize; i++) {
                values[i] = v;
                v += baseInterval + randomLongBetween(-maxJitter, maxJitter);
            }
            assertRoundTrip(values, blockSize);
        }
    }

    public void testRoundTripFuzzyRandom() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = randomLong();
            }
            assertRoundTrip(values, blockSize);
        }
    }

    public void testFullPipelineRoundTripWithOffsetBitPack() throws IOException {
        for (int iter = 0; iter < 20; iter++) {
            final int blockSize = randomBlockSize();
            final long start = randomLongBetween(1_000_000_000_000L, 2_000_000_000_000L);
            final long interval = randomLongBetween(1000, 60_000);
            final long maxJitter = Math.max(1, interval / 10);
            long[] values = new long[blockSize];
            long v = start;
            for (int i = 0; i < blockSize; i++) {
                values[i] = v;
                v += interval + randomLongBetween(-maxJitter, maxJitter);
            }
            final NumericEncoder encoder = NumericEncoder.fromConfig(PipelineConfig.forLongs(blockSize).deltaDelta().offset().bitPack());
            assertFullPipelineRoundTrip(values, encoder);
        }
    }

    public void testFullPipelineRoundTripWithPatchedPFor() throws IOException {
        for (int iter = 0; iter < 20; iter++) {
            final int blockSize = randomBlockSize();
            final long start = randomLongBetween(1_000_000_000_000L, 2_000_000_000_000L);
            final long interval = 10_000;
            long[] values = new long[blockSize];
            long v = start;
            for (int i = 0; i < blockSize; i++) {
                values[i] = v;
                if (randomIntBetween(0, 99) < 3) {
                    v += interval * randomLongBetween(50, 200);
                } else {
                    v += interval + randomLongBetween(-10, 10);
                }
            }
            final NumericEncoder encoder = NumericEncoder.fromConfig(
                PipelineConfig.forLongs(blockSize).deltaDelta().offset().patchedPFor().bitPack()
            );
            assertFullPipelineRoundTrip(values, encoder);
        }
    }

    public void testFullPipelineRoundTripConstantInterval() throws IOException {
        final int blockSize = randomBlockSize();
        final long start = 1_700_000_000_000L;
        final long interval = 15_000;
        long[] values = LongStream.iterate(start, v -> v + interval).limit(blockSize).toArray();
        final NumericEncoder encoder = NumericEncoder.fromConfig(PipelineConfig.forLongs(blockSize).deltaDelta().offset().bitPack());
        assertFullPipelineRoundTrip(values, encoder);
    }

    private void assertRoundTrip(final long[] original, int blockSize) throws IOException {
        assertRoundTrip(original, blockSize, StageId.DELTA_DELTA.id, DeltaDeltaCodecStage.INSTANCE, DeltaDeltaCodecStage.INSTANCE);
    }
}
