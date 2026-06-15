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
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.IntFunction;

public class SplitDeltaCounterStorageComparisonTests extends ESTestCase {

    private static final int[] BLOCK_SIZES = { 128, 512, 1024, 2048 };

    private static final long BASE_COUNTER = 1_000_000_000L;
    private static final long STEP_COUNT = 10_000L;
    private static final long TSID_RESET_VALUE = 100_000L;
    private static final long SLOW_STEP = 1L;

    public void testAscendingSingleBoundaryBlock() throws IOException {
        assertSizes(bs -> counterBoundaryBlock(bs, bs / 2), new long[] { 276, 1092, 2180, 4356 }, new long[] { 15, 16, 16, 16 });
    }

    public void testAscendingFourFlipBlock() throws IOException {
        assertSizes(bs -> counterMultiFlipBlock(bs, 4), new long[] { 308, 1220, 2436, 4868 }, new long[] { 33, 36, 37, 37 });
    }

    public void testAscendingKMaxFlipBlock() throws IOException {
        assertSizes(bs -> counterMultiFlipBlock(bs, 16), new long[] { 340, 1348, 2692, 5380 }, new long[] { 105, 117, 119, 120 });
    }

    public void testAscendingAboveKMaxFlipBlock() throws IOException {
        assertDeclinesAt(bs -> counterMultiFlipBlock(bs, 17), new long[] { 340, 1348, 2692, 5380 });
    }

    public void testAscendingFullyMonotonicBlock() throws IOException {
        assertDeclines(SplitDeltaCounterStorageComparisonTests::counterAscendingBlock);
    }

    public void testConstantBlock() throws IOException {
        assertDeclines(SplitDeltaCounterStorageComparisonTests::counterConstantBlock);
    }

    public void testAscendingBoundaryPositionSweep() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            for (int split = 2; split < blockSize - 1; split++) {
                final long[] values = counterBoundaryBlock(blockSize, split);
                final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
                final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
                assertTrue(
                    "SplitDelta must beat baseline at split="
                        + split
                        + " bs="
                        + blockSize
                        + " (splitDelta="
                        + splitDeltaSize
                        + ", delta="
                        + deltaSize
                        + ")",
                    splitDeltaSize < deltaSize
                );
            }
        }
    }

    public void testDescendingBoundaryPositionSweep() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            for (int split = 2; split < blockSize - 1; split++) {
                final long[] values = counterDescendingBoundaryBlock(blockSize, split);
                final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
                final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
                assertTrue(
                    "SplitDelta must beat baseline at split="
                        + split
                        + " bs="
                        + blockSize
                        + " (splitDelta="
                        + splitDeltaSize
                        + ", delta="
                        + deltaSize
                        + ")",
                    splitDeltaSize < deltaSize
                );
            }
        }
    }

    public void testDescendingSingleBoundaryBlock() throws IOException {
        assertSizes(bs -> counterDescendingBoundaryBlock(bs, bs / 2), new long[] { 281, 1097, 2185, 4361 }, new long[] { 17, 18, 18, 18 });
    }

    public void testDescendingFourFlipBlock() throws IOException {
        assertSizes(bs -> counterDescendingMultiFlipBlock(bs, 4), new long[] { 308, 1220, 2436, 4868 }, new long[] { 33, 37, 38, 38 });
    }

    public void testDescendingKMaxFlipBlock() throws IOException {
        assertSizes(bs -> counterDescendingMultiFlipBlock(bs, 16), new long[] { 340, 1348, 2692, 5380 }, new long[] { 105, 117, 119, 121 });
    }

    public void testDescendingAboveKMaxFlipBlock() throws IOException {
        assertDeclinesAt(bs -> counterDescendingMultiFlipBlock(bs, 17), new long[] { 340, 1348, 2692, 5380 });
    }

    public void testDescendingFullyMonotonicBlock() throws IOException {
        assertDeclines(SplitDeltaCounterStorageComparisonTests::counterDescendingBlock);
    }

    public void testSlowAscendingSingleBoundaryBlock() throws IOException {
        assertSizes(bs -> slowCounterBoundaryBlock(bs, bs / 2), new long[] { 514, 2050, 4098, 8194 }, new long[] { 13, 14, 14, 14 });
    }

    public void testSlowAscendingFourFlipBlock() throws IOException {
        assertSizes(bs -> slowCounterMultiFlipBlock(bs, 4), new long[] { 514, 2050, 4098, 8194 }, new long[] { 31, 34, 35, 35 });
    }

    public void testSlowAscendingKMaxFlipBlock() throws IOException {
        assertSizes(bs -> slowCounterMultiFlipBlock(bs, 16), new long[] { 642, 2562, 5122, 10242 }, new long[] { 103, 115, 117, 118 });
    }

    public void testSlowDescendingSingleBoundaryBlock() throws IOException {
        assertDeclinesAt(bs -> slowDescendingCounterBoundaryBlock(bs, bs / 2), new long[] { 33, 81, 145, 273 });
    }

    public void testSlowDescendingFourFlipBlock() throws IOException {
        assertSizes(bs -> slowDescendingCounterMultiFlipBlock(bs, 4), new long[] { 514, 2050, 4098, 8194 }, new long[] { 31, 34, 35, 35 });
    }

    public void testSlowDescendingKMaxFlipBlock() throws IOException {
        assertSizes(
            bs -> slowDescendingCounterMultiFlipBlock(bs, 16),
            new long[] { 642, 2562, 5122, 10242 },
            new long[] { 103, 115, 117, 118 }
        );
    }

    private void assertSizes(IntFunction<long[]> factory, long[] expectedDelta, long[] expectedSplitDelta) throws IOException {
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = factory.apply(blockSize);
            final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
            final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
            logger.info("bs={} delta={} splitDelta={}", blockSize, deltaSize, splitDeltaSize);
            assertEquals("delta bs=" + blockSize, expectedDelta[i], deltaSize);
            assertEquals("splitDelta bs=" + blockSize, expectedSplitDelta[i], splitDeltaSize);
        }
    }

    private void assertDeclinesAt(IntFunction<long[]> factory, long[] expectedDelta) throws IOException {
        assertSizes(factory, expectedDelta, expectedDelta);
    }

    private void assertDeclines(IntFunction<long[]> factory) throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = factory.apply(blockSize);
            final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
            final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
            logger.info("bs={} delta={} splitDelta={}", blockSize, deltaSize, splitDeltaSize);
            assertEquals("SplitDelta must not regress (bs=" + blockSize + ")", deltaSize, splitDeltaSize);
        }
    }

    private static PipelineConfig deltaPipeline(int blockSize) {
        return PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
    }

    private static PipelineConfig splitDeltaPipeline(int blockSize) {
        return PipelineConfig.forLongs(blockSize).splitDelta().delta().offset().gcd().bitPack();
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

    private static long[] counterBoundaryBlock(int blockSize, int split) {
        final long[] values = new long[blockSize];
        for (int i = 0; i < split; i++) {
            values[i] = BASE_COUNTER + (long) i * STEP_COUNT;
        }
        for (int i = split; i < blockSize; i++) {
            values[i] = TSID_RESET_VALUE + (long) (i - split) * STEP_COUNT;
        }
        return values;
    }

    private static long[] counterMultiFlipBlock(int blockSize, int flips) {
        final long[] values = new long[blockSize];
        final int subRunCount = flips + 1;
        final int baseSubRunLen = blockSize / subRunCount;
        final int remainder = blockSize % subRunCount;
        int pos = 0;
        for (int s = 0; s < subRunCount; s++) {
            final int subRunLen = baseSubRunLen + (s < remainder ? 1 : 0);
            final long subBase = TSID_RESET_VALUE + (long) (subRunCount - 1 - s) * BASE_COUNTER;
            for (int i = 0; i < subRunLen; i++) {
                values[pos++] = subBase + (long) i * STEP_COUNT;
            }
        }
        return values;
    }

    private static long[] counterAscendingBlock(int blockSize) {
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = BASE_COUNTER + (long) i * STEP_COUNT;
        }
        return values;
    }

    private static long[] counterConstantBlock(int blockSize) {
        final long[] values = new long[blockSize];
        Arrays.fill(values, BASE_COUNTER);
        return values;
    }

    private static long[] counterDescendingBoundaryBlock(int blockSize, int split) {
        final long[] values = new long[blockSize];
        final long firstTsidTop = BASE_COUNTER + (long) (split - 1) * STEP_COUNT;
        for (int i = 0; i < split; i++) {
            values[i] = firstTsidTop - (long) i * STEP_COUNT;
        }
        final long secondTsidTop = 2L * BASE_COUNTER;
        for (int i = split; i < blockSize; i++) {
            values[i] = secondTsidTop - (long) (i - split) * STEP_COUNT;
        }
        return values;
    }

    private static long[] counterDescendingBlock(int blockSize) {
        final long[] values = new long[blockSize];
        final long top = BASE_COUNTER + (long) (blockSize - 1) * STEP_COUNT;
        for (int i = 0; i < blockSize; i++) {
            values[i] = top - (long) i * STEP_COUNT;
        }
        return values;
    }

    private static long[] counterDescendingMultiFlipBlock(int blockSize, int flips) {
        final long[] values = new long[blockSize];
        final int subRunCount = flips + 1;
        final int baseSubRunLen = blockSize / subRunCount;
        final int remainder = blockSize % subRunCount;
        int pos = 0;
        for (int s = 0; s < subRunCount; s++) {
            final int subRunLen = baseSubRunLen + (s < remainder ? 1 : 0);
            final long subBase = TSID_RESET_VALUE + (long) s * BASE_COUNTER + (long) (subRunLen - 1) * STEP_COUNT;
            for (int i = 0; i < subRunLen; i++) {
                values[pos++] = subBase - (long) i * STEP_COUNT;
            }
        }
        return values;
    }

    private static long[] slowCounterBoundaryBlock(int blockSize, int split) {
        final long[] values = new long[blockSize];
        for (int i = 0; i < split; i++) {
            values[i] = BASE_COUNTER + (long) i * SLOW_STEP;
        }
        for (int i = split; i < blockSize; i++) {
            values[i] = TSID_RESET_VALUE + (long) (i - split) * SLOW_STEP;
        }
        return values;
    }

    private static long[] slowCounterMultiFlipBlock(int blockSize, int flips) {
        final long[] values = new long[blockSize];
        final int subRunCount = flips + 1;
        final int baseSubRunLen = blockSize / subRunCount;
        final int remainder = blockSize % subRunCount;
        int pos = 0;
        for (int s = 0; s < subRunCount; s++) {
            final int subRunLen = baseSubRunLen + (s < remainder ? 1 : 0);
            final long subBase = TSID_RESET_VALUE + (long) (subRunCount - 1 - s) * BASE_COUNTER;
            for (int i = 0; i < subRunLen; i++) {
                values[pos++] = subBase + (long) i * SLOW_STEP;
            }
        }
        return values;
    }

    private static long[] slowDescendingCounterBoundaryBlock(int blockSize, int split) {
        final long[] values = new long[blockSize];
        final long firstTsidTop = BASE_COUNTER + (long) (split - 1) * SLOW_STEP;
        for (int i = 0; i < split; i++) {
            values[i] = firstTsidTop - (long) i * SLOW_STEP;
        }
        final long secondTsidTop = TSID_RESET_VALUE + (long) (blockSize - split - 1) * SLOW_STEP;
        for (int i = split; i < blockSize; i++) {
            values[i] = secondTsidTop - (long) (i - split) * SLOW_STEP;
        }
        return values;
    }

    private static long[] slowDescendingCounterMultiFlipBlock(int blockSize, int flips) {
        final long[] values = new long[blockSize];
        final int subRunCount = flips + 1;
        final int baseSubRunLen = blockSize / subRunCount;
        final int remainder = blockSize % subRunCount;
        int pos = 0;
        for (int s = 0; s < subRunCount; s++) {
            final int subRunLen = baseSubRunLen + (s < remainder ? 1 : 0);
            final long subBase = TSID_RESET_VALUE + (long) s * BASE_COUNTER + (long) (subRunLen - 1) * SLOW_STEP;
            for (int i = 0; i < subRunLen; i++) {
                values[pos++] = subBase - (long) i * SLOW_STEP;
            }
        }
        return values;
    }
}
