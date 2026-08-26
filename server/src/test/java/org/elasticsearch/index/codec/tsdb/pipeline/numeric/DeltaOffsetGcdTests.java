/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class DeltaOffsetGcdTests extends ESTestCase {

    public void testCumulativeSumMulAllThreeStagesApply() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        long current = 1_000L;
        for (int i = 0; i < blockSize; i++) {
            values[i] = current;
            current += (i % 4 == 0) ? 100L : (i % 4 == 1) ? 200L : (i % 4 == 2) ? 300L : 200L;
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testCumulativeSumShiftAllThreeStagesApplyPow2Gcd() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        long current = 4_096L;
        for (int i = 0; i < blockSize; i++) {
            values[i] = current;
            current += (i % 3 == 0) ? 64L : (i % 3 == 1) ? 128L : 192L;
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testCumulativeSumShiftDeltaAndOffsetApplyGcdSkips() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        long current = 0L;
        for (int i = 0; i < blockSize; i++) {
            values[i] = current;
            current += randomIntBetween(1, 7);
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testLinearMulOffsetAndGcdApplyDeltaSkips() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        final long base = 1_500L;
        for (int i = 0; i < blockSize; i++) {
            values[i] = base + ((i * 7L) % 11L) * 300L;
        }
        for (int i = blockSize - 1; i > 0; i--) {
            final int j = randomIntBetween(0, i);
            final long tmp = values[i];
            values[i] = values[j];
            values[j] = tmp;
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testLinearShiftOffsetAndPow2GcdApplyDeltaSkips() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        final long base = 4_096L;
        for (int i = 0; i < blockSize; i++) {
            values[i] = base + ((i & 1) == 0 ? 0L : 32L * (1L + (i % 3L)));
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testNothingAppliedEarlyReturn() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        final long[] pattern = { 0L, 7L, 3L, 11L, 13L, 5L, 17L, 2L };
        for (int i = 0; i < blockSize; i++) {
            values[i] = pattern[i % pattern.length];
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testGcdOnlyAppliesNonPow2() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        final long[] pattern = { 0L, 300L, 600L };
        for (int i = 0; i < blockSize; i++) {
            values[i] = pattern[i % pattern.length];
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testGcdOnlyAppliesPow2() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        final long[] pattern = { 0L, 64L, 128L };
        for (int i = 0; i < blockSize; i++) {
            values[i] = pattern[i % pattern.length];
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testOffsetOnlyApplies() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        final long[] pattern = { 1_000L, 1_007L, 1_003L, 1_011L, 1_013L, 1_005L, 1_017L, 1_002L };
        for (int i = 0; i < blockSize; i++) {
            values[i] = pattern[i % pattern.length];
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testDeltaOnlyApplies() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        final long[] delta = { 0L, 3L, 0L, 2L, 0L, 7L };
        long current = 0L;
        for (int i = 0; i < blockSize; i++) {
            values[i] = current;
            current += delta[i % delta.length];
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testDeltaAndGcdApplyOffsetSkipsNonPow2() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = 100L * (i / 2);
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testDeltaAndGcdApplyOffsetSkipsPow2() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = 64L * (i / 2);
        }
        assertFusedRoundTrip(values, blockSize);
    }

    public void testRoundTripWithLeadingSplitDeltaStage() throws IOException {
        final int blockSize = 128;
        final int numBlocks = 2;
        final long[][] allValues = new long[numBlocks][];
        final long baseTimestamp = 1_700_000_000_000L;
        final long interval = 10_000L;
        final long boundaryJump = 240L * 60L * 1_000L;
        for (int b = 0; b < numBlocks; b++) {
            allValues[b] = new long[blockSize];
            final int boundary = blockSize / 2 + b;
            long current = baseTimestamp + (long) b * boundaryJump;
            for (int i = 0; i < boundary; i++) {
                allValues[b][i] = current - (long) i * interval;
            }
            final long secondStart = current + boundaryJump;
            for (int i = boundary; i < blockSize; i++) {
                allValues[b][i] = secondStart - (long) (i - boundary) * interval;
            }
        }

        final PipelineConfig config = PipelineConfig.forLongs(blockSize).splitDelta().delta().offset().gcd().bitPack();
        assertMultiBlockRoundTrip(config, allValues, blockSize);
    }

    public void testNonMatchingSuffixUsesUnfusedPath() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        long current = 0L;
        for (int i = 0; i < blockSize; i++) {
            values[i] = current;
            current += 50L * randomIntBetween(1, 5);
        }
        final PipelineConfig config = PipelineConfig.forLongs(blockSize).delta().gcd().bitPack();
        assertSingleBlockRoundTrip(config, values, blockSize, blockSize);
    }

    public void testPartialBlockRoundTrip() throws IOException {
        final int blockSize = 128;
        final int count = 73;
        final long[] values = new long[blockSize];
        long current = 1_000L;
        for (int i = 0; i < count; i++) {
            values[i] = current;
            current += 100L * (1L + (i % 5L));
        }
        assertFusedRoundTrip(values, blockSize, count);
    }

    private static void assertFusedRoundTrip(final long[] values, int blockSize) throws IOException {
        assertFusedRoundTrip(values, blockSize, values.length);
    }

    private static void assertFusedRoundTrip(final long[] values, int blockSize, int count) throws IOException {
        final PipelineConfig config = PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
        assertSingleBlockRoundTrip(config, values, blockSize, count);
    }

    private static void assertSingleBlockRoundTrip(final PipelineConfig config, final long[] values, int blockSize, int count)
        throws IOException {
        final long[] expected = Arrays.copyOf(values, blockSize);
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();

        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test")) {
            blockEncoder.encode(Arrays.copyOf(values, blockSize), count, out);
        }

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(encoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
        final long[] decoded = new long[blockSize];
        blockDecoder.decode(decoded, count, bufferOut.toDataInput());

        for (int i = 0; i < count; i++) {
            assertEquals("index " + i, expected[i], decoded[i]);
        }
    }

    private static void assertMultiBlockRoundTrip(final PipelineConfig config, final long[][] blocks, int blockSize) throws IOException {
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();
        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test")) {
            for (long[] block : blocks) {
                blockEncoder.encode(Arrays.copyOf(block, blockSize), blockSize, out);
            }
        }

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(encoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
        final ByteBuffersDataInput in = bufferOut.toDataInput();
        for (int b = 0; b < blocks.length; b++) {
            final long[] decoded = new long[blockSize];
            blockDecoder.decode(decoded, blockSize, in);
            assertArrayEquals("block " + b, blocks[b], decoded);
        }
    }
}
