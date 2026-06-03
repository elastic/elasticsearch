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

public class AlpDoubleStorageComparisonTests extends ESTestCase {

    private static final int[] BLOCK_SIZES = { 128, 512, 1024, 2048 };

    public void testIntegerLikeDoublesBlock() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = integerLikeDoublesBlock(blockSize);
            final long alpSize = encodeBlockSize(alpPipeline(blockSize), values);
            final long baselineSize = encodeBlockSize(baselinePipeline(blockSize), values);
            assertTrue(
                "ALP must beat baseline for integer doubles at blockSize="
                    + blockSize
                    + " (alp="
                    + alpSize
                    + ", baseline="
                    + baselineSize
                    + ")",
                alpSize < baselineSize
            );
        }
    }

    public void testCurrencyLikeBlock() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = currencyLikeBlock(blockSize);
            final long alpSize = encodeBlockSize(alpPipeline(blockSize), values);
            final long baselineSize = encodeBlockSize(baselinePipeline(blockSize), values);
            assertTrue(
                "ALP must beat baseline for currency-like data at blockSize="
                    + blockSize
                    + " (alp="
                    + alpSize
                    + ", baseline="
                    + baselineSize
                    + ")",
                alpSize < baselineSize
            );
        }
    }

    public void testSensorLikeBlock() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = sensorLikeBlock(blockSize);
            final long alpSize = encodeBlockSize(alpPipeline(blockSize), values);
            final long baselineSize = encodeBlockSize(baselinePipeline(blockSize), values);
            assertTrue(
                "ALP must beat baseline for sensor-like data at blockSize="
                    + blockSize
                    + " (alp="
                    + alpSize
                    + ", baseline="
                    + baselineSize
                    + ")",
                alpSize < baselineSize
            );
        }
    }

    public void testConstantDoubleBlock() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = new long[blockSize];
            Arrays.fill(values, NumericUtils.doubleToSortableLong(42.5));
            final long alpSize = encodeBlockSize(alpPipeline(blockSize), values);
            final long baselineSize = encodeBlockSize(baselinePipeline(blockSize), values);
            assertTrue(
                "ALP must not regress on a constant block at blockSize="
                    + blockSize
                    + " (alp="
                    + alpSize
                    + ", baseline="
                    + baselineSize
                    + ")",
                alpSize <= baselineSize
            );
        }
    }

    public void testIrrationalBlockDoesNotRegress() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = irrationalBlock(blockSize);
            final long alpSize = encodeBlockSize(alpPipeline(blockSize), values);
            final long baselineSize = encodeBlockSize(baselinePipeline(blockSize), values);
            // ALP skips on irrational data; storage must match the baseline within a handful of
            // bytes for the bitmap and stage descriptor overhead.
            assertTrue(
                "ALP must not regress more than 64 bytes vs baseline on irrational data at blockSize="
                    + blockSize
                    + " (alp="
                    + alpSize
                    + ", baseline="
                    + baselineSize
                    + ")",
                alpSize <= baselineSize + 64
            );
        }
    }

    public void testMonotonicAscendingIntegerDoublesNoCatastrophicRegression() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = monotonicAscendingIntegerDoublesBlock(blockSize);
            assertBoundedRegression("monotonic ascending integer doubles", blockSize, values);
        }
    }

    public void testMonotonicAscendingCurrencyNoCatastrophicRegression() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = monotonicAscendingCurrencyBlock(blockSize);
            assertBoundedRegression("monotonic ascending currency", blockSize, values);
        }
    }

    public void testMonotonicDescendingIntegerDoublesNoCatastrophicRegression() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = monotonicDescendingIntegerDoublesBlock(blockSize);
            assertBoundedRegression("monotonic descending integer doubles", blockSize, values);
        }
    }

    public void testSignCrossingOscillatingBlock() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = signCrossingOscillatingBlock(blockSize);
            final long alpSize = encodeBlockSize(alpPipeline(blockSize), values);
            final long baselineSize = encodeBlockSize(baselinePipeline(blockSize), values);
            assertTrue(
                "ALP must beat baseline for sign-crossing 2dp data at blockSize="
                    + blockSize
                    + " (alp="
                    + alpSize
                    + ", baseline="
                    + baselineSize
                    + ")",
                alpSize < baselineSize
            );
        }
    }

    public void testAggregateAcrossPatterns() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[][] blocks = {
                integerLikeDoublesBlock(blockSize),
                currencyLikeBlock(blockSize),
                sensorLikeBlock(blockSize),
                constantBlock(blockSize, 42.5),
                irrationalBlock(blockSize) };
            final long alpTotal = encodeBlocksTotalSize(alpPipeline(blockSize), blocks);
            final long baselineTotal = encodeBlocksTotalSize(baselinePipeline(blockSize), blocks);
            assertTrue(
                "ALP must beat baseline aggregated across patterns at blockSize="
                    + blockSize
                    + " (alp="
                    + alpTotal
                    + ", baseline="
                    + baselineTotal
                    + ")",
                alpTotal < baselineTotal
            );
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

    private static final int MAX_MONOTONIC_REGRESSION_FACTOR = 20;

    private void assertBoundedRegression(String label, int blockSize, long[] values) throws IOException {
        final long alpSize = encodeBlockSize(alpPipeline(blockSize), values);
        final long baselineSize = encodeBlockSize(baselinePipeline(blockSize), values);
        assertTrue(
            "ALP must stay within "
                + MAX_MONOTONIC_REGRESSION_FACTOR
                + "x of baseline on "
                + label
                + " at blockSize="
                + blockSize
                + " (alp="
                + alpSize
                + ", baseline="
                + baselineSize
                + ")",
            alpSize <= baselineSize * MAX_MONOTONIC_REGRESSION_FACTOR
        );
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
