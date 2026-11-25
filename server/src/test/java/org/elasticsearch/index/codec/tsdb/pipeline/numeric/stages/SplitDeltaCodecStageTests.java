/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.StageSpec;

import java.io.IOException;
import java.util.Arrays;

public class SplitDeltaCodecStageTests extends AbstractTransformStageTestCase {

    private static SplitDeltaCodecStage defaultStage() {
        return new SplitDeltaCodecStage(StageSpec.SplitDeltaStage.DEFAULT_K_MAX);
    }

    public void testIdMatchesStageId() {
        assertEquals(StageId.SPLIT_DELTA_STAGE.id, defaultStage().id());
    }

    public void testInvalidKMaxThrows() {
        expectThrows(IllegalArgumentException.class, () -> new SplitDeltaCodecStage(0));
        expectThrows(IllegalArgumentException.class, () -> new SplitDeltaCodecStage(-1));
        expectThrows(IllegalArgumentException.class, () -> new SplitDeltaCodecStage(StageSpec.SplitDeltaStage.MAX_K_MAX + 1));
    }

    public void testForBlockSizeKMaxScalesWithBlockSize() {
        assertEquals(4, SplitDeltaCodecStage.forBlockSize(128).kMax());
        assertEquals(16, SplitDeltaCodecStage.forBlockSize(512).kMax());
        assertEquals(32, SplitDeltaCodecStage.forBlockSize(1024).kMax());
        assertEquals(64, SplitDeltaCodecStage.forBlockSize(2048).kMax());
    }

    public void testForBlockSizeClampsToBounds() {
        assertEquals(4, SplitDeltaCodecStage.forBlockSize(1).kMax());
        assertEquals(4, SplitDeltaCodecStage.forBlockSize(64).kMax());
        assertEquals(64, SplitDeltaCodecStage.forBlockSize(4096).kMax());
        assertEquals(64, SplitDeltaCodecStage.forBlockSize(Integer.MAX_VALUE).kMax());
    }

    public void testForBlockSizeRoundTripAtProductionBlockSize() throws IOException {
        final SplitDeltaCodecStage stage = SplitDeltaCodecStage.forBlockSize(512);
        final long[] values = piecewiseDescending(512, 8);
        assertTransformRoundTrip(stage, values);
    }

    public void testForBlockSizeRejectsInvalidBlockSize() {
        expectThrows(IllegalArgumentException.class, () -> SplitDeltaCodecStage.forBlockSize(0));
        expectThrows(IllegalArgumentException.class, () -> SplitDeltaCodecStage.forBlockSize(-1));
    }

    public void testFullyMonotonicAscendingSkips() {
        final int blockSize = randomBlockSize();
        final long[] values = randomMonotonicIncreasing(blockSize);
        assertStageSkipped(defaultStage(), values, blockSize);
    }

    public void testFullyMonotonicDescendingSkips() {
        final int blockSize = randomBlockSize();
        final long[] values = randomMonotonicDecreasing(blockSize);
        assertStageSkipped(defaultStage(), values, blockSize);
    }

    public void testConstantValuesSkips() {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, randomLong());
        assertStageSkipped(defaultStage(), values, blockSize);
    }

    public void testValueCountBelowFourSkips() {
        assertStageSkipped(defaultStage(), new long[] { 100, 50, 60 }, 3);
        assertStageSkipped(defaultStage(), new long[] { 100, 50 }, 2);
        assertStageSkipped(defaultStage(), new long[] { 100 }, 1);
    }

    public void testAcceptAndDeclineAcrossKRange() throws IOException {
        for (int k = 1; k <= 17; k++) {
            try {
                assertSplitDeltaAtK(k);
            } catch (AssertionError e) {
                throw new AssertionError("failed for k=" + k + ": " + e.getMessage(), e);
            }
        }
    }

    private static void assertSplitDeltaAtK(int k) throws IOException {
        final SplitDeltaCodecStage stage = new SplitDeltaCodecStage(16);
        if (k > 16) {
            assertStageSkipped(stage, piecewiseDescending(128, k), 128);
            return;
        }
        assertTransformRoundTrip(stage, piecewiseDescending(128, k));
    }

    public void testKMaxFlipsRoundTrip() throws IOException {
        final int kMax = randomIntBetween(2, 8);
        final SplitDeltaCodecStage stage = new SplitDeltaCodecStage(kMax);
        final int blockSize = 128;
        final long[] values = piecewiseDescending(blockSize, kMax);
        assertTransformRoundTrip(stage, values);
    }

    public void testTooManyFlipsSkips() {
        final int kMax = 2;
        final SplitDeltaCodecStage stage = new SplitDeltaCodecStage(kMax);
        final int blockSize = 128;
        final long[] values = piecewiseDescending(blockSize, kMax + 1);
        assertStageSkipped(stage, values, blockSize);
    }

    public void testTrailingAnomalySkips() {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize - 1; i++) {
            values[i] = 1_000_000L - i;
        }
        values[blockSize - 1] = 5_000_000L;
        assertStageSkipped(defaultStage(), values, blockSize);
    }

    public void testLeadingAnomalySkips() {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        values[0] = 5_000_000L;
        for (int i = 1; i < blockSize; i++) {
            values[i] = 1_000_000L - i;
        }
        assertStageSkipped(defaultStage(), values, blockSize);
    }

    public void testTsdbBoundaryBlockRoundTrip() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        final int firstFlip = blockSize / 2;
        for (int i = 0; i < firstFlip; i++) {
            values[i] = 1_000_000L - i;
        }
        for (int i = firstFlip; i < blockSize; i++) {
            values[i] = 2_000_000L - (i - firstFlip);
        }
        assertTransformRoundTrip(defaultStage(), values);
    }

    public void testAscendingBoundaryBlockRoundTrip() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        final int firstFlip = blockSize / 2;
        for (int i = 0; i < firstFlip; i++) {
            values[i] = 2_000_000L + i;
        }
        for (int i = firstFlip; i < blockSize; i++) {
            values[i] = 1_000_000L + (i - firstFlip);
        }
        assertTransformRoundTrip(defaultStage(), values);
    }

    public void testAscendingMultiFlipRoundTrip() throws IOException {
        final int blockSize = 128;
        final long[] values = piecewiseAscending(blockSize, 3);
        assertTransformRoundTrip(defaultStage(), values);
    }

    public void testAscendingKMaxFlipsRoundTrip() throws IOException {
        final int kMax = randomIntBetween(2, 8);
        final SplitDeltaCodecStage stage = new SplitDeltaCodecStage(kMax);
        final int blockSize = 128;
        final long[] values = piecewiseAscending(blockSize, kMax);
        assertTransformRoundTrip(stage, values);
    }

    public void testFlatStepsInsideSubRunRoundTrip() throws IOException {
        final long[] values = new long[16];
        values[0] = 1000;
        values[1] = 999;
        values[2] = 999;
        values[3] = 998;
        values[4] = 998;
        values[5] = 997;
        values[6] = 996;
        values[7] = 995;
        values[8] = 5000;
        values[9] = 5000;
        values[10] = 4999;
        values[11] = 4999;
        values[12] = 4998;
        values[13] = 4997;
        values[14] = 4996;
        values[15] = 4995;
        assertTransformRoundTrip(defaultStage(), values);
    }

    public void testCustomKMaxRoundTrip() throws IOException {
        final SplitDeltaCodecStage stage = new SplitDeltaCodecStage(4);
        final long[] values = piecewiseDescending(128, 3);
        assertTransformRoundTrip(stage, values);
    }

    public void testMultiBlockRoundTripWithArrayReuse() throws IOException {
        final int blockSize = 128;
        final int numBlocks = randomIntBetween(3, 6);
        final long[][] blocks = new long[numBlocks][];
        for (int b = 0; b < numBlocks; b++) {
            final int k = randomIntBetween(1, 4);
            blocks[b] = piecewiseDescending(blockSize, k);
        }
        assertMultiBlockTransformRoundTrip(defaultStage(), blocks);
    }

    public void testFlipPositionSweepRoundTrip() throws IOException {
        final int blockSize = 128;
        for (int p = 2; p < blockSize - 1; p++) {
            final long[] values = blockWithFlipAt(blockSize, p);
            assertTransformRoundTrip(defaultStage(), values);
        }
        final long[] trailing = blockWithFlipAt(blockSize, blockSize - 1);
        assertStageSkipped(defaultStage(), trailing, blockSize);
    }

    public void testTrailingFlipAfterMidFlipSkips() {
        final long[] values = { 3L, 2L, 1L, 0L, 6L, 5L, 4L, 999L };
        assertStageSkipped(defaultStage(), values, values.length);
    }

    public void testKMaxFlipsWithTrailingFlipDeclines() {
        final int blockSize = 128;
        final long[] values = piecewiseDescending(blockSize, StageSpec.SplitDeltaStage.DEFAULT_K_MAX);
        values[blockSize - 1] = values[blockSize - 2] + 999_999_999L;
        assertStageSkipped(defaultStage(), values, blockSize);
    }

    private static long[] blockWithFlipAt(final int blockSize, final int flipPosition) {
        final long[] values = new long[blockSize];
        final long anchorA = 1_000_000_000L;
        final long anchorB = 2_000_000_000L;
        for (int i = 0; i < flipPosition; i++) {
            values[i] = anchorA - i;
        }
        for (int i = flipPosition; i < blockSize; i++) {
            values[i] = anchorB - (i - flipPosition);
        }
        return values;
    }

    public void testPartialBlockRoundTrip() throws IOException {
        final int blockSize = 128;
        final int count = randomIntBetween(8, blockSize - 1);
        final long[] block = new long[blockSize];
        final long[] active = piecewiseDescending(count, 2);
        System.arraycopy(active, 0, block, 0, count);
        assertPartialBlockTransformRoundTrip(defaultStage(), block, count);
    }

    private static long[] piecewiseDescending(int valueCount, int flips) {
        if (valueCount < (flips + 1) * 2) {
            throw new IllegalArgumentException("valueCount=" + valueCount + " too small for flips=" + flips);
        }
        final long[] values = new long[valueCount];
        final int subRunCount = flips + 1;
        final int baseSubRunLen = valueCount / subRunCount;
        final int remainder = valueCount % subRunCount;
        long start = 1_000_000_000L;
        int pos = 0;
        for (int s = 0; s < subRunCount; s++) {
            final int subRunLen = baseSubRunLen + (s < remainder ? 1 : 0);
            for (int i = 0; i < subRunLen; i++) {
                values[pos++] = start - i;
            }
            start = values[pos - 1] + 500_000L;
        }
        return values;
    }

    private static long[] piecewiseAscending(int valueCount, int flips) {
        if (valueCount < (flips + 1) * 2) {
            throw new IllegalArgumentException("valueCount=" + valueCount + " too small for flips=" + flips);
        }
        final long[] values = new long[valueCount];
        final int subRunCount = flips + 1;
        final int baseSubRunLen = valueCount / subRunCount;
        final int remainder = valueCount % subRunCount;
        long start = 1_000_000_000L;
        int pos = 0;
        for (int s = 0; s < subRunCount; s++) {
            final int subRunLen = baseSubRunLen + (s < remainder ? 1 : 0);
            for (int i = 0; i < subRunLen; i++) {
                values[pos++] = start + i;
            }
            start = values[pos - 1] - 500_000L;
        }
        return values;
    }

    private static void assertPartialBlockTransformRoundTrip(final SplitDeltaCodecStage stage, final long[] block, final int count)
        throws IOException {
        final long[] active = Arrays.copyOf(block, count);
        assertTransformRoundTrip(stage, active);
    }
}
