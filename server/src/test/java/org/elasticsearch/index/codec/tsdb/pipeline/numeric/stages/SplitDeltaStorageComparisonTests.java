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
import java.util.Random;

public class SplitDeltaStorageComparisonTests extends ESTestCase {

    // NOTE: BS=128 is the non-TSDB default; BS=512 is the production block size for
    // @timestamp in TSDB indices; BS=1024 and BS=2048 capture the larger block sizes
    // SplitDelta unlocks by neutralizing the boundary block payload penalty.
    private static final int[] BLOCK_SIZES = { 128, 512, 1024, 2048 };
    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;
    private static final long INTERVAL_MS = 10_000L;
    private static final long BOUNDARY_JUMP_MS = 240L * 60L * 1000L;
    // NOTE: jitter must be strictly less than INTERVAL_MS / 2 so all inter-sample diffs
    // within a sub-run remain negative, preserving the descending direction the stage
    // expects. With INTERVAL_MS=10000 and JITTER_MS=1000, diffs lie in [-12000, -8000].
    private static final long JITTER_MS = 1_000L;
    private static final long JITTER_SEED = 42L;

    public void testSingleBoundaryBlock() throws IOException {
        final long[] expectedDelta = { 186, 714, 1418, 3082 };
        final long[] expectedSplitDelta = { 19, 20, 20, 20 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = tsdbBoundaryBlock(blockSize, blockSize / 2);
            assertEquals(expectedDelta[i], encodeBlockSize(deltaPipeline(blockSize), values));
            assertEquals(expectedSplitDelta[i], encodeBlockSize(splitDeltaPipeline(blockSize), values));
        }
    }

    public void testFourFlipBlock() throws IOException {
        final long[] expectedDelta = { 218, 842, 1674, 3338 };
        final long[] expectedSplitDelta = { 40, 43, 44, 44 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = tsdbMultiFlipBlock(blockSize, 4);
            assertEquals(expectedDelta[i], encodeBlockSize(deltaPipeline(blockSize), values));
            assertEquals(expectedSplitDelta[i], encodeBlockSize(splitDeltaPipeline(blockSize), values));
        }
    }

    public void testKMaxFlipBlock() throws IOException {
        final long[] expectedDelta = { 250, 970, 1930, 3850 };
        final long[] expectedSplitDelta = { 124, 136, 138, 139 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = tsdbMultiFlipBlock(blockSize, 16);
            assertEquals(expectedDelta[i], encodeBlockSize(deltaPipeline(blockSize), values));
            assertEquals(expectedSplitDelta[i], encodeBlockSize(splitDeltaPipeline(blockSize), values));
        }
    }

    public void testFullyDescendingBlock() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = tsdbDescendingBlock(blockSize);
            assertEquals(11, encodeBlockSize(deltaPipeline(blockSize), values));
            assertEquals(11, encodeBlockSize(splitDeltaPipeline(blockSize), values));
        }
    }

    public void testConstantBlock() throws IOException {
        for (int blockSize : BLOCK_SIZES) {
            final long[] values = new long[blockSize];
            Arrays.fill(values, BASE_TIMESTAMP);
            assertEquals(8, encodeBlockSize(deltaPipeline(blockSize), values));
            assertEquals(8, encodeBlockSize(splitDeltaPipeline(blockSize), values));
        }
    }

    public void testTooManyFlipsBlock() throws IOException {
        final long[] expected = { 250, 970, 1930, 3850 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = tsdbMultiFlipBlock(blockSize, 17);
            assertEquals(expected[i], encodeBlockSize(deltaPipeline(blockSize), values));
            assertEquals(expected[i], encodeBlockSize(splitDeltaPipeline(blockSize), values));
        }
    }

    public void testJitteredBoundaryBlock() throws IOException {
        final long[] expectedDelta = { 392, 2056, 4104, 8200 };
        final long[] expectedSplitDelta = { 211, 788, 1556, 3092 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = jitteredBoundaryBlock(blockSize, blockSize / 2);
            assertEquals(expectedDelta[i], encodeBlockSize(deltaPipeline(blockSize), values));
            assertEquals(expectedSplitDelta[i], encodeBlockSize(splitDeltaPipeline(blockSize), values));
        }
    }

    public void testJitteredFullyDescendingBlock() throws IOException {
        final long[] expected = { 203, 779, 1547, 3083 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[] values = jitteredDescendingBlock(blockSize);
            assertEquals(expected[i], encodeBlockSize(deltaPipeline(blockSize), values));
            assertEquals(expected[i], encodeBlockSize(splitDeltaPipeline(blockSize), values));
        }
    }

    public void testBlockSequenceWithVaryingFlipCounts() throws IOException {
        final long[] expectedDelta = { 1106, 4274, 8498, 17202 };
        final long[] expectedSplitDelta = { 263, 286, 290, 292 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[][] blocks = blocksWithVaryingFlipCounts(blockSize);
            assertEquals(expectedDelta[i], encodeBlocksTotalSize(deltaPipeline(blockSize), blocks));
            assertEquals(expectedSplitDelta[i], encodeBlocksTotalSize(splitDeltaPipeline(blockSize), blocks));
        }
    }

    public void testJitteredBlockSequenceWithVaryingFlipCounts() throws IOException {
        final long[] expectedDelta = { 2472, 10280, 20520, 41000 };
        final long[] expectedSplitDelta = { 1223, 4126, 7970, 15652 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[][] blocks = jitteredBlocksWithVaryingFlipCounts(blockSize);
            assertEquals(expectedDelta[i], encodeBlocksTotalSize(deltaPipeline(blockSize), blocks));
            assertEquals(expectedSplitDelta[i], encodeBlocksTotalSize(splitDeltaPipeline(blockSize), blocks));
        }
    }

    public void testAggregateOverHundredBlocksWithTenBoundaries() throws IOException {
        final long[] expectedDelta = { 2850, 8130, 15170, 31810 };
        final long[] expectedSplitDelta = { 1180, 1190, 1190, 1190 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[][] blocks = boundaryAndDescendingBlockMix(blockSize, 10);
            assertEquals(expectedDelta[i], encodeBlocksTotalSize(deltaPipeline(blockSize), blocks));
            assertEquals(expectedSplitDelta[i], encodeBlocksTotalSize(splitDeltaPipeline(blockSize), blocks));
        }
    }

    public void testAggregateOverHundredBlocksWithTwentyBoundaries() throws IOException {
        final long[] expectedDelta = { 4600, 15160, 29240, 62520 };
        final long[] expectedSplitDelta = { 1260, 1280, 1280, 1280 };
        for (int i = 0; i < BLOCK_SIZES.length; i++) {
            final int blockSize = BLOCK_SIZES[i];
            final long[][] blocks = boundaryAndDescendingBlockMix(blockSize, 20);
            assertEquals(expectedDelta[i], encodeBlocksTotalSize(deltaPipeline(blockSize), blocks));
            assertEquals(expectedSplitDelta[i], encodeBlocksTotalSize(splitDeltaPipeline(blockSize), blocks));
        }
    }

    private static PipelineConfig deltaPipeline(int blockSize) {
        return PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
    }

    private static PipelineConfig splitDeltaPipeline(int blockSize) {
        return PipelineConfig.forLongs(blockSize).withSplitDelta().delta().offset().gcd().bitPack();
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

    private static long[] tsdbBoundaryBlock(int blockSize, int split) {
        final long[] values = new long[blockSize];
        for (int i = 0; i < split; i++) {
            values[i] = BASE_TIMESTAMP - (long) i * INTERVAL_MS;
        }
        for (int i = split; i < blockSize; i++) {
            values[i] = BASE_TIMESTAMP + BOUNDARY_JUMP_MS - (long) (i - split) * INTERVAL_MS;
        }
        return values;
    }

    private static long[] tsdbDescendingBlock(int blockSize) {
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = BASE_TIMESTAMP - (long) i * INTERVAL_MS;
        }
        return values;
    }

    private static long[] tsdbMultiFlipBlock(int blockSize, int flips) {
        final long[] values = new long[blockSize];
        final int subRunCount = flips + 1;
        final int baseSubRunLen = blockSize / subRunCount;
        final int remainder = blockSize % subRunCount;
        int pos = 0;
        for (int s = 0; s < subRunCount; s++) {
            final int subRunLen = baseSubRunLen + (s < remainder ? 1 : 0);
            final long subBase = BASE_TIMESTAMP + (long) s * BOUNDARY_JUMP_MS;
            for (int i = 0; i < subRunLen; i++) {
                values[pos++] = subBase - (long) i * INTERVAL_MS;
            }
        }
        return values;
    }

    private static long[] jitteredBoundaryBlock(int blockSize, int split) {
        final Random rng = new Random(JITTER_SEED);
        final long[] values = new long[blockSize];
        for (int i = 0; i < split; i++) {
            values[i] = BASE_TIMESTAMP - (long) i * INTERVAL_MS + nextJitter(rng);
        }
        for (int i = split; i < blockSize; i++) {
            values[i] = BASE_TIMESTAMP + BOUNDARY_JUMP_MS - (long) (i - split) * INTERVAL_MS + nextJitter(rng);
        }
        return values;
    }

    private static long[] jitteredDescendingBlock(int blockSize) {
        final Random rng = new Random(JITTER_SEED);
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = BASE_TIMESTAMP - (long) i * INTERVAL_MS + nextJitter(rng);
        }
        return values;
    }

    private static long[] jitteredMultiFlipBlock(int blockSize, int flips, Random rng) {
        final long[] values = new long[blockSize];
        final int subRunCount = flips + 1;
        final int baseSubRunLen = blockSize / subRunCount;
        final int remainder = blockSize % subRunCount;
        int pos = 0;
        for (int s = 0; s < subRunCount; s++) {
            final int subRunLen = baseSubRunLen + (s < remainder ? 1 : 0);
            final long subBase = BASE_TIMESTAMP + (long) s * BOUNDARY_JUMP_MS;
            for (int i = 0; i < subRunLen; i++) {
                values[pos++] = subBase - (long) i * INTERVAL_MS + nextJitter(rng);
            }
        }
        return values;
    }

    private static long[][] blocksWithVaryingFlipCounts(int blockSize) {
        final int[] flipCounts = { 1, 3, 5, 8, 12 };
        final long[][] blocks = new long[flipCounts.length][];
        for (int i = 0; i < flipCounts.length; i++) {
            blocks[i] = tsdbMultiFlipBlock(blockSize, flipCounts[i]);
        }
        return blocks;
    }

    private static long[][] jitteredBlocksWithVaryingFlipCounts(int blockSize) {
        final Random rng = new Random(JITTER_SEED);
        final int[] flipCounts = { 1, 3, 5, 8, 12 };
        final long[][] blocks = new long[flipCounts.length][];
        for (int i = 0; i < flipCounts.length; i++) {
            blocks[i] = jitteredMultiFlipBlock(blockSize, flipCounts[i], rng);
        }
        return blocks;
    }

    private static long[][] boundaryAndDescendingBlockMix(int blockSize, int boundaryCount) {
        final long[][] blocks = new long[100][];
        for (int b = 0; b < 100; b++) {
            blocks[b] = (b < boundaryCount) ? tsdbBoundaryBlock(blockSize, blockSize / 2) : tsdbDescendingBlock(blockSize);
        }
        return blocks;
    }

    private static long nextJitter(final Random rng) {
        return rng.nextInt((int) (2 * JITTER_MS + 1)) - JITTER_MS;
    }
}
