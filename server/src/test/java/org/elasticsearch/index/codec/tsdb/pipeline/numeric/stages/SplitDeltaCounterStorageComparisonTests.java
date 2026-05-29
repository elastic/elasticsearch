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

public class SplitDeltaCounterStorageComparisonTests extends ESTestCase {

    // NOTE: BS=128 is the non-TSDB default; BS=512 is the production block size for
    // metric fields in TSDB indices; BS=1024 and BS=2048 capture the larger block sizes
    // SplitDelta unlocks by neutralizing the boundary block payload penalty.
    private static final int[] BLOCK_SIZES = { 128, 512, 1024, 2048 };

    // NOTE: a realistic accumulated packet/byte counter for a long-running process sits
    // in the billions; STEP_COUNT models the per-sample increment. TSID_RESET_VALUE is
    // the small value the counter restarts from at a tsid boundary.
    private static final long BASE_COUNTER = 1_000_000_000L;
    private static final long STEP_COUNT = 10_000L;
    private static final long TSID_RESET_VALUE = 100L;

    public void testSingleBoundaryBlock() throws IOException {
        final long[] expectedDelta = { 387, 1539, 3075, 6147 };
        final long[] expectedSplitDelta = { 15, 16, 16, 16 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = counterBoundaryBlock(blockSize, blockSize / 2);
            final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
            final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
            logger.info("[testSingleBoundaryBlock] bs={} delta={} splitDelta={}", blockSize, deltaSize, splitDeltaSize);
            assertEquals(expectedDelta[i], deltaSize);
            assertEquals(expectedSplitDelta[i], splitDeltaSize);
        }
    }

    public void testFourFlipBlock() throws IOException {
        final long[] expectedDelta = { 515, 2051, 4099, 8195 };
        final long[] expectedSplitDelta = { 33, 36, 37, 37 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = counterMultiFlipBlock(blockSize, 4);
            final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
            final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
            logger.info("[testFourFlipBlock] bs={} delta={} splitDelta={}", blockSize, deltaSize, splitDeltaSize);
            assertEquals(expectedDelta[i], deltaSize);
            assertEquals(expectedSplitDelta[i], splitDeltaSize);
        }
    }

    public void testKMaxFlipBlock() throws IOException {
        final long[] expectedDelta = { 515, 2051, 4099, 8195 };
        final long[] expectedSplitDelta = { 105, 117, 119, 120 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = counterMultiFlipBlock(blockSize, 16);
            final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
            final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
            logger.info("[testKMaxFlipBlock] bs={} delta={} splitDelta={}", blockSize, deltaSize, splitDeltaSize);
            assertEquals(expectedDelta[i], deltaSize);
            assertEquals(expectedSplitDelta[i], splitDeltaSize);
        }
    }

    public void testFullyMonotonicBlock() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = counterAscendingBlock(blockSize);
            final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
            final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
            logger.info("[testFullyMonotonicBlock] bs={} delta={} splitDelta={}", blockSize, deltaSize, splitDeltaSize);
            assertEquals(10, deltaSize);
            assertEquals("fully monotonic block must not regress under SplitDelta (bs=" + blockSize + ")", deltaSize, splitDeltaSize);
        }
    }

    public void testConstantBlock() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = new long[blockSize];
            Arrays.fill(values, BASE_COUNTER);
            final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
            final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
            logger.info("[testConstantBlock] bs={} delta={} splitDelta={}", blockSize, deltaSize, splitDeltaSize);
            assertEquals(7, deltaSize);
            assertEquals("constant block must not regress under SplitDelta (bs=" + blockSize + ")", deltaSize, splitDeltaSize);
        }
    }

    public void testTooManyFlipsBlock() throws IOException {
        final long[] expected = { 515, 2051, 4099, 8195 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = counterMultiFlipBlock(blockSize, 17);
            final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
            final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
            logger.info("[testTooManyFlipsBlock] bs={} delta={} splitDelta={}", blockSize, deltaSize, splitDeltaSize);
            assertEquals(expected[i], deltaSize);
            assertEquals("block with k>kMax flips must decline and match the baseline (bs=" + blockSize + ")", deltaSize, splitDeltaSize);
        }
    }

    public void testBoundaryPositionSweep() throws IOException {
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

    public void testDescendingSingleBoundaryBlock() throws IOException {
        final long[] expectedDelta = { 281, 1097, 2185, 4361 };
        final long[] expectedSplitDelta = { 17, 18, 18, 18 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = counterDescendingBoundaryBlock(blockSize, blockSize / 2);
            final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
            final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
            logger.info("[testDescendingSingleBoundaryBlock] bs={} delta={} splitDelta={}", blockSize, deltaSize, splitDeltaSize);
            assertEquals(expectedDelta[i], deltaSize);
            assertEquals(expectedSplitDelta[i], splitDeltaSize);
        }
    }

    public void testDescendingFourFlipBlock() throws IOException {
        final long[] expectedDelta = { 515, 2051, 4099, 8195 };
        final long[] expectedSplitDelta = { 33, 36, 38, 38 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = counterDescendingMultiFlipBlock(blockSize, 4);
            final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
            final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
            logger.info("[testDescendingFourFlipBlock] bs={} delta={} splitDelta={}", blockSize, deltaSize, splitDeltaSize);
            assertEquals(expectedDelta[i], deltaSize);
            assertEquals(expectedSplitDelta[i], splitDeltaSize);
        }
    }

    public void testFullyDescendingBlock() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = counterDescendingBlock(blockSize);
            final long deltaSize = encodeBlockSize(deltaPipeline(blockSize), values);
            final long splitDeltaSize = encodeBlockSize(splitDeltaPipeline(blockSize), values);
            logger.info("[testFullyDescendingBlock] bs={} delta={} splitDelta={}", blockSize, deltaSize, splitDeltaSize);
            assertEquals("fully descending block must not regress under SplitDelta (bs=" + blockSize + ")", deltaSize, splitDeltaSize);
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

    private static long[] counterAscendingBlock(int blockSize) {
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = BASE_COUNTER + (long) i * STEP_COUNT;
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
}
