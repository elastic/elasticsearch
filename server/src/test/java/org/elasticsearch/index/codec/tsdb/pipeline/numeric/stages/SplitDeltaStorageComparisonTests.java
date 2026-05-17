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

    private static final int BLOCK_SIZE = 128;
    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;
    private static final long INTERVAL_MS = 10_000L;
    private static final long BOUNDARY_JUMP_MS = 240L * 60L * 1000L;
    // NOTE: jitter must be strictly less than INTERVAL_MS / 2 so all inter-sample diffs
    // within a sub-run remain negative, preserving the descending direction the stage
    // expects. With INTERVAL_MS=10000 and JITTER_MS=1000, diffs lie in [-12000, -8000].
    private static final long JITTER_MS = 1_000L;
    private static final long JITTER_SEED = 42L;

    public void testSingleBoundaryBlock() throws IOException {
        final long[] values = tsdbBoundaryBlock(BLOCK_SIZE, BLOCK_SIZE / 2);
        assertEquals(186, encodeBlockSize(deltaPipeline(), values));
        assertEquals(19, encodeBlockSize(splitDeltaPipeline(), values));
    }

    public void testFourFlipBlock() throws IOException {
        final long[] values = tsdbMultiFlipBlock(BLOCK_SIZE, 4);
        assertEquals(218, encodeBlockSize(deltaPipeline(), values));
        assertEquals(40, encodeBlockSize(splitDeltaPipeline(), values));
    }

    public void testKMaxFlipBlock() throws IOException {
        final long[] values = tsdbMultiFlipBlock(BLOCK_SIZE, 16);
        assertEquals(250, encodeBlockSize(deltaPipeline(), values));
        assertEquals(124, encodeBlockSize(splitDeltaPipeline(), values));
    }

    public void testFullyDescendingBlock() throws IOException {
        final long[] values = tsdbDescendingBlock(BLOCK_SIZE);
        assertEquals(11, encodeBlockSize(deltaPipeline(), values));
        assertEquals(11, encodeBlockSize(splitDeltaPipeline(), values));
    }

    public void testConstantBlock() throws IOException {
        final long[] values = new long[BLOCK_SIZE];
        Arrays.fill(values, BASE_TIMESTAMP);
        assertEquals(8, encodeBlockSize(deltaPipeline(), values));
        assertEquals(8, encodeBlockSize(splitDeltaPipeline(), values));
    }

    public void testTooManyFlipsBlock() throws IOException {
        final long[] values = tsdbMultiFlipBlock(BLOCK_SIZE, 17);
        assertEquals(250, encodeBlockSize(deltaPipeline(), values));
        assertEquals(250, encodeBlockSize(splitDeltaPipeline(), values));
    }

    public void testJitteredBoundaryBlock() throws IOException {
        final long[] values = jitteredBoundaryBlock(BLOCK_SIZE, BLOCK_SIZE / 2);
        assertEquals(392, encodeBlockSize(deltaPipeline(), values));
        assertEquals(211, encodeBlockSize(splitDeltaPipeline(), values));
    }

    public void testJitteredFullyDescendingBlock() throws IOException {
        final long[] values = jitteredDescendingBlock(BLOCK_SIZE);
        assertEquals(203, encodeBlockSize(deltaPipeline(), values));
        assertEquals(203, encodeBlockSize(splitDeltaPipeline(), values));
    }

    public void testMixedKValuesAcrossBlocks() throws IOException {
        final long[][] blocks = mixedKBlocks();
        assertEquals(1106, encodeBlocksTotalSize(deltaPipeline(), blocks));
        assertEquals(263, encodeBlocksTotalSize(splitDeltaPipeline(), blocks));
    }

    public void testJitteredMixedKValuesAcrossBlocks() throws IOException {
        final long[][] blocks = jitteredMixedKBlocks();
        assertEquals(2472, encodeBlocksTotalSize(deltaPipeline(), blocks));
        assertEquals(1223, encodeBlocksTotalSize(splitDeltaPipeline(), blocks));
    }

    public void testAggregateOverHundredBlocksWithTenBoundaries() throws IOException {
        final long[][] blocks = new long[100][];
        for (int b = 0; b < 100; b++) {
            blocks[b] = (b < 10) ? tsdbBoundaryBlock(BLOCK_SIZE, BLOCK_SIZE / 2) : tsdbDescendingBlock(BLOCK_SIZE);
        }
        assertEquals(2850, encodeBlocksTotalSize(deltaPipeline(), blocks));
        assertEquals(1180, encodeBlocksTotalSize(splitDeltaPipeline(), blocks));
    }

    private static PipelineConfig deltaPipeline() {
        return PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().gcd().bitPack();
    }

    private static PipelineConfig splitDeltaPipeline() {
        return PipelineConfig.forLongs(BLOCK_SIZE).withSplitDelta().delta().offset().gcd().bitPack();
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

    private static long[][] mixedKBlocks() {
        final int[] ks = { 1, 3, 5, 8, 12 };
        final long[][] blocks = new long[ks.length][];
        for (int i = 0; i < ks.length; i++) {
            blocks[i] = tsdbMultiFlipBlock(BLOCK_SIZE, ks[i]);
        }
        return blocks;
    }

    private static long[][] jitteredMixedKBlocks() {
        final Random rng = new Random(JITTER_SEED);
        final int[] ks = { 1, 3, 5, 8, 12 };
        final long[][] blocks = new long[ks.length][];
        for (int i = 0; i < ks.length; i++) {
            blocks[i] = jitteredMultiFlipBlock(BLOCK_SIZE, ks[i], rng);
        }
        return blocks;
    }

    private static long nextJitter(final Random rng) {
        return rng.nextInt((int) (2 * JITTER_MS + 1)) - JITTER_MS;
    }
}
