/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.IntFunction;

public class AlpDoubleCounterStorageComparisonTests extends ESTestCase {

    private static final int[] BLOCK_SIZES = { 128, 512, 1024, 2048 };

    public void testCounterAscendingWithResetsBlock() throws IOException {
        assertSizes(
            AlpDoubleCounterStorageComparisonTests::counterAscendingWithResetsBlock,
            new long[] { 265, 1033, 2057, 4105 },
            new long[] { 10, 28, 52, 100 }
        );
    }

    public void testCounterDescendingWithSpikesBlock() throws IOException {
        assertSizes(
            AlpDoubleCounterStorageComparisonTests::counterDescendingWithSpikesBlock,
            new long[] { 131, 467, 915, 1811 },
            new long[] { 12, 36, 68, 132 }
        );
    }

    public void testMonotonicAscendingIntegerDoublesBlock() throws IOException {
        assertSizes(
            AlpDoubleCounterStorageComparisonTests::monotonicAscendingIntegerDoublesBlock,
            new long[] { 41, 89, 153, 537 },
            new long[] { 8, 8, 8, 8 }
        );
    }

    private void assertSizes(IntFunction<long[]> factory, long[] expectedBaseline, long[] expectedAlpCounter) throws IOException {
        final long[] actualBaseline = new long[BLOCK_SIZES.length];
        final long[] actualAlpCounter = new long[BLOCK_SIZES.length];
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int bs = BLOCK_SIZES[i];
            final long[] values = factory.apply(bs);
            actualBaseline[i] = encodeBlockSize(baselinePipeline(bs), values);
            actualAlpCounter[i] = encodeBlockSize(alpCounterPipeline(bs), values);
            logger.info("bs={} baseline={} alpCounter={}", bs, actualBaseline[i], actualAlpCounter[i]);
        }
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int bs = BLOCK_SIZES[i];
            assertEquals("baseline bs=" + bs, expectedBaseline[i], actualBaseline[i]);
            assertEquals("alpCounter bs=" + bs, expectedAlpCounter[i], actualAlpCounter[i]);
        }
    }

    private static PipelineConfig alpCounterPipeline(int blockSize) {
        final int kMax = Math.clamp((long) blockSize / 32, 4, 64);
        return PipelineConfig.forDoubles(blockSize).alpDoubleStage().splitDelta(kMax).delta().offset().gcd().bitPack();
    }

    private static PipelineConfig baselinePipeline(int blockSize) {
        return PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
    }

    private static long encodeBlockSize(PipelineConfig config, long[] values) throws IOException {
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();
        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test")) {
            blockEncoder.encode(Arrays.copyOf(values, values.length), values.length, out);
        }
        return bufferOut.size();
    }

    private static long[] counterAscendingWithResetsBlock(int size) {
        // NOTE: monotonic ascending integer counter that resets to zero every 64 values,
        // mirroring a byte-count metric that wraps on service restart or overflow.
        final long[] values = new long[size];
        double accumulator = 0.0;
        for (int i = 0; i < size; i++) {
            if (i > 0 && i % 64 == 0) {
                accumulator = 0.0;
            } else {
                accumulator += 1.0;
            }
            values[i] = NumericUtils.doubleToSortableLong(accumulator);
        }
        return values;
    }

    private static long[] counterDescendingWithSpikesBlock(int size) {
        // NOTE: monotonic descending counter (e.g. queue depth draining) that spikes back to
        // the base value every 64 values, mirroring a drain-and-refill metric shape.
        final long[] values = new long[size];
        final double base = 1000.0;
        double accumulator = base;
        for (int i = 0; i < size; i++) {
            if (i > 0 && i % 64 == 0) {
                accumulator = base;
            } else {
                accumulator -= 1.0;
            }
            values[i] = NumericUtils.doubleToSortableLong(accumulator);
        }
        return values;
    }

    private static long[] monotonicAscendingIntegerDoublesBlock(int size) {
        final long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) (1000 + i));
        }
        return values;
    }
}
