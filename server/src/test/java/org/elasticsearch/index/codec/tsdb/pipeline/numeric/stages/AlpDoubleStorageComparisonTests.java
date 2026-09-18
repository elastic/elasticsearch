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

public class AlpDoubleStorageComparisonTests extends ESTestCase {

    private static final int[] BLOCK_SIZES = { 128, 512, 1024, 2048 };

    public void testIntegerLikeDoublesBlock() throws IOException {
        assertSizes(
            AlpDoubleStorageComparisonTests::integerLikeDoublesBlock,
            new long[] { 131, 467, 915, 1811 },
            new long[] { 102, 390, 774, 1542 }
        );
    }

    public void testCurrencyLikeBlock() throws IOException {
        assertSizes(
            AlpDoubleStorageComparisonTests::currencyLikeBlock,
            new long[] { 780, 3084, 6156, 12300 },
            new long[] { 275, 1133, 2258, 4526 }
        );
    }

    public void testSensorLikeBlock() throws IOException {
        assertSizes(
            AlpDoubleStorageComparisonTests::sensorLikeBlock,
            new long[] { 780, 3084, 6156, 12300 },
            new long[] { 258, 1038, 2089, 4199 }
        );
    }

    public void testConstantDoubleBlock() throws IOException {
        assertSizes(bs -> constantBlock(bs, 42.5), new long[] { 12, 12, 12, 12 }, new long[] { 7, 7, 7, 7 });
    }

    public void testIrrationalBlock() throws IOException {
        assertDeclines(AlpDoubleStorageComparisonTests::irrationalBlock);
    }

    public void testMonotonicAscendingIntegerDoublesBlock() throws IOException {
        assertSizes(
            AlpDoubleStorageComparisonTests::monotonicAscendingIntegerDoublesBlock,
            new long[] { 41, 89, 153, 537 },
            new long[] { 8, 8, 8, 8 }
        );
    }

    public void testMonotonicAscendingCurrencyBlock() throws IOException {
        assertDeclines(AlpDoubleStorageComparisonTests::monotonicAscendingCurrencyBlock);
    }

    public void testMonotonicDescendingIntegerDoublesBlock() throws IOException {
        assertSizes(
            AlpDoubleStorageComparisonTests::monotonicDescendingIntegerDoublesBlock,
            new long[] { 41, 89, 153, 537 },
            new long[] { 8, 8, 8, 8 }
        );
    }

    public void testSignCrossingOscillatingBlock() throws IOException {
        assertSizes(
            AlpDoubleStorageComparisonTests::signCrossingOscillatingBlock,
            new long[] { 1035, 4107, 8203, 16395 },
            new long[] { 102, 390, 774, 1542 }
        );
    }

    public void testAggregateAcrossPatterns() throws IOException {
        // Five-block aggregate sums per blockSize. ALP totals reflect the cross-block
        // (e, f) cache carrying state between consecutive blocks; the baseline pipeline
        // has no cross-block state so its totals are exactly the sum of per-pattern rows.
        final long[] expectedBaseline = { 2611, 10243, 20419, 40771 };
        final long[] expectedAlp = { 1550, 6164, 12308, 24622 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int bs = BLOCK_SIZES[i];
            final long[][] blocks = {
                integerLikeDoublesBlock(bs),
                currencyLikeBlock(bs),
                sensorLikeBlock(bs),
                constantBlock(bs, 42.5),
                irrationalBlock(bs) };
            final long baselineSize = encodeBlocksTotalSize(baselinePipeline(bs), blocks);
            final long alpSize = encodeBlocksTotalSize(alpPipeline(bs), blocks);
            logger.info("aggregate bs={} baseline={} alp={}", bs, baselineSize, alpSize);
            assertEquals("baseline bs=" + bs, expectedBaseline[i], baselineSize);
            assertEquals("alp bs=" + bs, expectedAlp[i], alpSize);
        }
    }

    private void assertSizes(IntFunction<long[]> factory, long[] expectedBaseline, long[] expectedAlp) throws IOException {
        final long[] actualBaseline = new long[BLOCK_SIZES.length];
        final long[] actualAlp = new long[BLOCK_SIZES.length];
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int bs = BLOCK_SIZES[i];
            final long[] values = factory.apply(bs);
            actualBaseline[i] = encodeBlockSize(baselinePipeline(bs), values);
            actualAlp[i] = encodeBlockSize(alpPipeline(bs), values);
            logger.info("bs={} baseline={} alp={}", bs, actualBaseline[i], actualAlp[i]);
        }
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int bs = BLOCK_SIZES[i];
            assertEquals("baseline bs=" + bs, expectedBaseline[i], actualBaseline[i]);
            assertEquals("alp bs=" + bs, expectedAlp[i], actualAlp[i]);
        }
    }

    private void assertDeclines(IntFunction<long[]> factory) throws IOException {
        for (int bs : BLOCK_SIZES) {
            final long[] values = factory.apply(bs);
            final long baselineSize = encodeBlockSize(baselinePipeline(bs), values);
            final long alpSize = encodeBlockSize(alpPipeline(bs), values);
            logger.info("bs={} baseline={} alp={}", bs, baselineSize, alpSize);
            assertEquals("ALP must decline on this shape (bs=" + bs + ")", baselineSize, alpSize);
        }
    }

    private static PipelineConfig alpPipeline(int blockSize) {
        return PipelineConfig.forDoubles(blockSize).alpDoubleStage().delta().offset().gcd().bitPack();
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

    private static long encodeBlocksTotalSize(PipelineConfig config, long[][] blocks) throws IOException {
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();
        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test")) {
            for (long[] block : blocks) {
                blockEncoder.encode(Arrays.copyOf(block, block.length), block.length, out);
            }
        }
        return bufferOut.size();
    }

    private static long[] monotonicAscendingIntegerDoublesBlock(int size) {
        final long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) (1000 + i));
        }
        return values;
    }

    private static long[] monotonicDescendingIntegerDoublesBlock(int size) {
        final long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) (1000 + size - i));
        }
        return values;
    }

    private static long[] monotonicAscendingCurrencyBlock(int size) {
        final long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) (10_000 + i) / 100.0);
        }
        return values;
    }

    private static long[] signCrossingOscillatingBlock(int size) {
        final long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) ((i % 41) - 20) / 100.0);
        }
        return values;
    }

    private static long[] integerLikeDoublesBlock(int size) {
        final long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) (60 + (i % 41) - 20));
        }
        return values;
    }

    private static long[] currencyLikeBlock(int size) {
        final long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) (10_000 + (i % 41) - 20) / 100.0);
        }
        return values;
    }

    private static long[] sensorLikeBlock(int size) {
        final long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) (2250 + (i % 21) - 10) / 100.0);
        }
        return values;
    }

    private static long[] constantBlock(int size, double value) {
        final long[] values = new long[size];
        Arrays.fill(values, NumericUtils.doubleToSortableLong(value));
        return values;
    }

    private static long[] irrationalBlock(int size) {
        final long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Math.sqrt(i + 2) * Math.PI);
        }
        return values;
    }

}
