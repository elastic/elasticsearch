/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.datasources.SplitCoalescer.COALESCING_THRESHOLD;
import static org.elasticsearch.xpack.esql.datasources.SplitCoalescer.DEFAULT_MAX_FILES_PER_GROUP;
import static org.elasticsearch.xpack.esql.datasources.SplitCoalescer.DEFAULT_OPEN_COST_BYTES;

public class SplitCoalescerTests extends ESTestCase {

    public void testNullInputThrows() {
        expectThrows(IllegalArgumentException.class, () -> SplitCoalescer.coalesce(null));
    }

    public void testEmptyInputReturnsEmpty() {
        List<ExternalSplit> result = SplitCoalescer.coalesce(List.of());
        assertTrue(result.isEmpty());
    }

    public void testBelowThresholdReturnsUnchanged() {
        List<ExternalSplit> splits = makeSplits(COALESCING_THRESHOLD - 1);
        assertEquals(splits, SplitCoalescer.coalesce(splits));
    }

    public void testExactlyAtThresholdReturnsUnchanged() {
        List<ExternalSplit> splits = makeSplits(COALESCING_THRESHOLD);
        assertEquals(splits, SplitCoalescer.coalesce(splits));
    }

    public void testCoalesceNeverReturnsTheCallersList() {
        // Callers replace the contents of the list they passed in. Handing back that same list would make them
        // clear the list they are copying from, silently emptying the scan, so declining to group must still
        // produce a list of our own.
        List<ExternalSplit> belowThreshold = makeSplits(COALESCING_THRESHOLD - 1);
        assertNotSame(belowThreshold, SplitCoalescer.coalesce(belowThreshold));
        List<ExternalSplit> aboveThreshold = makeSplits(COALESCING_THRESHOLD + 1);
        assertNotSame(aboveThreshold, SplitCoalescer.coalesce(aboveThreshold));
    }

    public void testShouldCoalesceOwnsTheThresholdRule() {
        assertFalse(SplitCoalescer.shouldCoalesce(0));
        assertFalse(SplitCoalescer.shouldCoalesce(COALESCING_THRESHOLD - 1));
        assertFalse(SplitCoalescer.shouldCoalesce(COALESCING_THRESHOLD));
        assertTrue(SplitCoalescer.shouldCoalesce(COALESCING_THRESHOLD + 1));
    }

    public void testSizeBasedGrouping() {
        int count = 100;
        long fileSize = 10 * 1024 * 1024; // 10 MB each
        long targetGroupSize = 128 * 1024 * 1024; // 128 MB
        List<ExternalSplit> splits = makeSplits(count, fileSize);

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, targetGroupSize, 8);

        int totalChildren = countTotalLeaves(result);
        assertEquals(count, totalChildren);
        assertTrue("Expected fewer groups than original splits", result.size() < count);

        for (ExternalSplit split : result) {
            if (split instanceof CoalescedSplit coalesced) {
                assertTrue(
                    "Coalesced split should not exceed target size by more than one file",
                    coalesced.estimatedSizeInBytes() <= targetGroupSize + fileSize
                );
            }
        }
    }

    public void testCountBasedFallbackWhenNoSizeInfo() {
        int count = 100;
        int targetGroupCount = 8;
        List<ExternalSplit> splits = makeSplitsWithoutSize(count);

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, targetGroupCount);

        int totalChildren = countTotalLeaves(result);
        assertEquals(count, totalChildren);
        assertTrue("Expected at most targetGroupCount groups", result.size() <= targetGroupCount);
    }

    public void testSingleLargeFileLeftAlone() {
        long largeSize = 256 * 1024 * 1024; // 256 MB, larger than target
        long targetGroupSize = 128 * 1024 * 1024;

        List<ExternalSplit> splits = new ArrayList<>();
        splits.add(makeFileSplit(0, largeSize));
        for (int i = 1; i <= COALESCING_THRESHOLD; i++) {
            splits.add(makeFileSplit(i, 1024));
        }

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, targetGroupSize, 8);

        boolean foundLarge = false;
        for (ExternalSplit split : result) {
            if (split instanceof FileSplit fs && fs.length() == largeSize) {
                foundLarge = true;
            }
        }
        assertTrue("Large file should remain as standalone split", foundLarge);
        assertEquals(COALESCING_THRESHOLD + 1, countTotalLeaves(result));
    }

    public void testPreservesAllChildren() {
        int count = 64;
        List<ExternalSplit> splits = makeSplits(count, 10 * 1024 * 1024);

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits);

        Set<ExternalSplit> originalSet = new HashSet<>(splits);
        Set<ExternalSplit> resultLeaves = new HashSet<>();
        for (ExternalSplit split : result) {
            if (split instanceof CoalescedSplit coalesced) {
                resultLeaves.addAll(coalesced.children());
            } else {
                resultLeaves.add(split);
            }
        }
        assertEquals(originalSet, resultLeaves);
    }

    public void testInvalidTargetGroupCountThrows() {
        List<ExternalSplit> splits = makeSplits(COALESCING_THRESHOLD + 1);
        expectThrows(IllegalArgumentException.class, () -> SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 0));
    }

    public void testInvalidTargetGroupSizeBytesThrows() {
        List<ExternalSplit> splits = makeSplits(COALESCING_THRESHOLD + 1);
        expectThrows(IllegalArgumentException.class, () -> SplitCoalescer.coalesce(splits, 0, 8));
    }

    public void testParallelismFloorSpreadsTinyFiles() {
        int count = 100;
        int parallelism = 14;
        // 1 KB files all fit one 128 MB bin, so without a floor they collapse to a single group.
        List<ExternalSplit> splits = makeSplits(count, 1024);

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 8, parallelism);

        assertEquals("tiny files must be spread across at least the parallelism floor", parallelism, result.size());
        assertEquals(count, countTotalLeaves(result));
    }

    public void testParallelismFloorCappedBySplitCount() {
        int count = COALESCING_THRESHOLD + 8; // above the threshold so coalescing runs
        int parallelism = 100;                // more than the number of files
        List<ExternalSplit> splits = makeSplits(count, 1024);

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 8, parallelism);

        assertEquals("floor cannot exceed the number of input splits", count, result.size());
        assertEquals(count, countTotalLeaves(result));
    }

    public void testSizeRaisesGroupCountAboveParallelismFloor() {
        int count = 100;
        long fileSize = 10 * 1024 * 1024;  // 10 MB
        long target = 128 * 1024 * 1024;   // ~12 files per bin -> ~8 bins by size alone
        int parallelism = 4;               // lower than the size-required bin count
        List<ExternalSplit> splits = makeSplits(count, fileSize);

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, target, 8, parallelism);

        int sizeBins = (int) Math.ceil((count * (double) fileSize) / target);
        assertTrue("size must raise the group count above the parallelism floor", result.size() >= sizeBins);
        assertEquals(count, countTotalLeaves(result));
        for (ExternalSplit split : result) {
            if (split instanceof CoalescedSplit coalesced) {
                assertTrue("size budget must still be respected under the floor", coalesced.estimatedSizeInBytes() <= target + fileSize);
            }
        }
    }

    public void testParallelismFloorPreservesAllChildren() {
        int count = 100;
        int parallelism = 14;
        List<ExternalSplit> splits = makeSplits(count, 1024);

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 8, parallelism);

        Set<ExternalSplit> originalSet = new HashSet<>(splits);
        Set<ExternalSplit> resultLeaves = new HashSet<>();
        for (ExternalSplit split : result) {
            if (split instanceof CoalescedSplit coalesced) {
                resultLeaves.addAll(coalesced.children());
            } else {
                resultLeaves.add(split);
            }
        }
        assertEquals(originalSet, resultLeaves);
    }

    public void testParallelismFloorWithoutSizeInfo() {
        int count = 100;
        int parallelism = 14;
        List<ExternalSplit> splits = makeSplitsWithoutSize(count);

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 8, parallelism);

        assertEquals("count-based grouping must also honor the parallelism floor", parallelism, result.size());
        assertEquals(count, countTotalLeaves(result));
    }

    public void testFloorKeepsOversizedStandaloneAndSpreadsTiny() {
        long target = 128 * 1024 * 1024;
        int parallelism = 14;
        List<ExternalSplit> splits = new ArrayList<>();
        splits.add(makeFileSplit(0, 200L * 1024 * 1024)); // oversized
        splits.add(makeFileSplit(1, 300L * 1024 * 1024)); // oversized
        for (int i = 2; i < 100; i++) {
            splits.add(makeFileSplit(i, 1024));
        }

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, target, 8, parallelism);

        assertEquals(parallelism, result.size());
        assertEquals(100, countTotalLeaves(result));
        int standaloneOversized = 0;
        for (ExternalSplit split : result) {
            if (split instanceof FileSplit fs && fs.length() >= target) {
                standaloneOversized++;
            }
            if (split instanceof CoalescedSplit coalesced) {
                assertTrue("coalesced groups must stay under the size budget", coalesced.estimatedSizeInBytes() <= target + 1024);
            }
        }
        assertEquals("both oversized files stay standalone", 2, standaloneOversized);
    }

    public void testFloorProducesBalancedGroups() {
        int count = 100;
        int parallelism = 14;
        List<ExternalSplit> splits = makeSplits(count, 1024);

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 8, parallelism);

        assertEquals(parallelism, result.size());
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (ExternalSplit split : result) {
            int leaves = split instanceof CoalescedSplit coalesced ? coalesced.children().size() : 1;
            min = Math.min(min, leaves);
            max = Math.max(max, leaves);
        }
        assertTrue("groups should be balanced within one leaf, got min=" + min + " max=" + max, max - min <= 1);
    }

    public void testFloorSpreadsZeroSizedFilesEvenly() {
        int count = 100;
        int parallelism = 14;
        // Zero-byte files all tie on load, so the least-loaded tie-break must still round-robin them across groups
        // rather than piling every file after the seed into the first group.
        List<ExternalSplit> splits = makeSplits(count, 0);

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 8, parallelism);

        assertEquals(parallelism, result.size());
        assertEquals(count, countTotalLeaves(result));
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (ExternalSplit split : result) {
            int leaves = split instanceof CoalescedSplit coalesced ? coalesced.children().size() : 1;
            min = Math.min(min, leaves);
            max = Math.max(max, leaves);
        }
        assertTrue("zero-sized files should be balanced within one leaf, got min=" + min + " max=" + max, max - min <= 1);
    }

    public void testFloorBalancesFileCountAcrossMixedSizes() {
        // A few larger files plus many tiny ones must not pile every tiny file into the single group seeded by a
        // tiny file. Read parallelism over small files is bounded by per-file opens, so groups are balanced by leaf
        // count, not by bytes: balancing by bytes alone would leave one group holding all 100 tiny files.
        long target = 128 * 1024 * 1024;
        int floor = 6;
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            splits.add(makeFileSplit(i, 10 * 1024 * 1024)); // 10 MB
        }
        for (int i = 5; i < 105; i++) {
            splits.add(makeFileSplit(i, 1024)); // 1 KB
        }

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, target, 8, floor);

        assertEquals(floor, result.size());
        assertEquals(105, countTotalLeaves(result));
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (ExternalSplit split : result) {
            int leaves = split instanceof CoalescedSplit coalesced ? coalesced.children().size() : 1;
            min = Math.min(min, leaves);
            max = Math.max(max, leaves);
        }
        assertTrue("groups must be balanced by leaf count, got min=" + min + " max=" + max, max - min <= 1);
    }

    public void testFloorStopsFillingGroupsAtSizeBudget() {
        // A near-budget file seeds one group. Balancing purely by leaf count would keep steering tiny files onto
        // that group whenever its count is the smallest, pushing it far past the budget. The fill must stop once a
        // group reaches the budget and send the remaining files to groups that still have room.
        long target = 100L * 1024 * 1024; // 100 MB
        long tiny = 1024 * 1024;          // 1 MB
        int floor = 4;
        List<ExternalSplit> splits = new ArrayList<>();
        splits.add(makeFileSplit(0, 95L * 1024 * 1024)); // 95 MB, just under budget
        for (int i = 1; i <= 105; i++) {
            splits.add(makeFileSplit(i, tiny));
        }

        // The default file cap would emit more bins than this floor on 1 MB leaves, skipping the floor path
        // this test is about. Lift the cap so spreadLeastLoaded still runs.
        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, target, 8, floor, Integer.MAX_VALUE);

        assertEquals(floor, result.size());
        assertEquals(106, countTotalLeaves(result));
        for (ExternalSplit split : result) {
            assertTrue(
                "no group may exceed the size budget by more than one file, got " + split.estimatedSizeInBytes(),
                split.estimatedSizeInBytes() <= target + tiny
            );
        }
    }

    public void testFloorOfOneMatchesDefaultGrouping() {
        int count = 100;
        List<ExternalSplit> splits = makeSplits(count, 10 * 1024 * 1024);

        List<ExternalSplit> defaultGrouping = SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 8);
        List<ExternalSplit> floored = SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 8, 1);

        assertEquals(defaultGrouping, floored);
    }

    public void testInvalidMinGroupCountThrows() {
        List<ExternalSplit> splits = makeSplits(COALESCING_THRESHOLD + 1);
        expectThrows(IllegalArgumentException.class, () -> SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 8, 0));
    }

    public void testBelowThresholdIgnoresMinGroups() {
        List<ExternalSplit> splits = makeSplits(COALESCING_THRESHOLD - 1);
        assertEquals(splits, SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 8, 14));
    }

    public void testInvalidParamsThrowEvenBelowThreshold() {
        // Validation must fire before the early-return size guard so the same invalid argument is always rejected.
        List<ExternalSplit> belowThreshold = makeSplits(COALESCING_THRESHOLD - 1);
        expectThrows(IllegalArgumentException.class, () -> SplitCoalescer.coalesce(belowThreshold, 128 * 1024 * 1024, 8, 0));
        expectThrows(IllegalArgumentException.class, () -> SplitCoalescer.coalesce(belowThreshold, 0, 8, 1));
        expectThrows(IllegalArgumentException.class, () -> SplitCoalescer.coalesce(belowThreshold, 128 * 1024 * 1024, 0, 1));
        expectThrows(IllegalArgumentException.class, () -> SplitCoalescer.coalesce(belowThreshold, 128 * 1024 * 1024, 8, 1, 0));
    }

    public void testNoGroupExceedsTheFileCountBudget() {
        // ClickBench-zstd leaf sizes pack 39 files under the 128 MiB byte budget alone. The file cap must bind
        // first so no driver is handed more than DEFAULT_MAX_FILES_PER_GROUP leaves.
        long target = 128L * 1024 * 1024;
        List<ExternalSplit> splits = makeSplits(3264, 3_386_719L);

        assertEquals(39, maxLeaves(SplitCoalescer.coalesce(splits, target, 8, 1, Integer.MAX_VALUE)));

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, target, 8, 1, DEFAULT_MAX_FILES_PER_GROUP);

        assertEquals(3264, countTotalLeaves(result));
        assertEquals("file cap must bind on this corpus (byte budget would allow 39)", DEFAULT_MAX_FILES_PER_GROUP, maxLeaves(result));
    }

    public void testByteBudgetStillBindsOnLargerFiles() {
        // Uncompressed ClickBench leaves fill the 128 MiB byte budget at 15 files, below the file cap.
        long target = 128L * 1024 * 1024;
        List<ExternalSplit> splits = makeSplits(3264, 8_512_192L);

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, target, 8, 1, DEFAULT_MAX_FILES_PER_GROUP);

        assertEquals(3264, countTotalLeaves(result));
        assertEquals("byte budget must still bind first on larger files", 15, maxLeaves(result));
        for (ExternalSplit split : result) {
            assertTrue("size budget must still be respected, got " + split.estimatedSizeInBytes(), split.estimatedSizeInBytes() <= target);
        }
    }

    public void testCompressionNoLongerScalesLargestGroup() {
        // Same two corpora as the defect: without a file cap, compression packed 39 files vs 15 uncompressed.
        // The default cap bounds the compressed side at 32; the byte budget still binds first on uncompressed.
        // The issue's 1.1 leaf-count ratio cannot hold at cap=32 because uncompressed stays byte-bound at 15.
        long target = 128L * 1024 * 1024;
        List<ExternalSplit> compressed = makeSplits(3264, 3_386_719L);
        List<ExternalSplit> uncompressed = makeSplits(3264, 8_512_192L);

        int maxCompressed = maxLeaves(SplitCoalescer.coalesce(compressed, target, 8, 1));
        int maxUncompressed = maxLeaves(SplitCoalescer.coalesce(uncompressed, target, 8, 1));

        assertEquals(DEFAULT_MAX_FILES_PER_GROUP, maxCompressed);
        assertEquals(15, maxUncompressed);
    }

    public void testClaimOrderPutsCostlyGroupsFirst() {
        // BFD emits the exact-budget standalone first. Claim cost (bytes + 4 MiB per leaf) must invert that:
        // a 32-leaf group of 4 MiB files costs ~256 MiB and must be claimed before the 128 MiB standalone
        // (~132 MiB) and before cheap 1 KiB leftovers.
        long target = 128L * 1024 * 1024;
        List<ExternalSplit> splits = new ArrayList<>();
        splits.add(makeFileSplit(0, target));
        for (int i = 1; i <= DEFAULT_MAX_FILES_PER_GROUP; i++) {
            splits.add(makeFileSplit(i, DEFAULT_OPEN_COST_BYTES));
        }
        for (int i = DEFAULT_MAX_FILES_PER_GROUP + 1; i <= DEFAULT_MAX_FILES_PER_GROUP + 40; i++) {
            splits.add(makeFileSplit(i, 1024));
        }

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, target, 8, 1);

        assertEquals(1 + DEFAULT_MAX_FILES_PER_GROUP + 40, countTotalLeaves(result));
        assertEquals(DEFAULT_MAX_FILES_PER_GROUP, leafCount(result.get(0)));
        assertEquals(DEFAULT_MAX_FILES_PER_GROUP * DEFAULT_OPEN_COST_BYTES, result.get(0).estimatedSizeInBytes());
        assertTrue(
            "exact-budget standalone must follow the costlier 32-leaf group",
            result.get(1) instanceof FileSplit standalone && standalone.length() == target
        );

        long previousCost = Long.MAX_VALUE;
        boolean sawFullFileCapGroup = false;
        for (int i = 0; i < result.size(); i++) {
            ExternalSplit group = result.get(i);
            long cost = claimCost(group);
            assertTrue(
                "claim cost must be non-increasing, at index " + i + " cost=" + cost + " prev=" + previousCost,
                cost <= previousCost
            );
            previousCost = cost;
            int leaves = leafCount(group);
            if (leaves == DEFAULT_MAX_FILES_PER_GROUP) {
                sawFullFileCapGroup = true;
            }
            if (leaves < DEFAULT_MAX_FILES_PER_GROUP && group instanceof CoalescedSplit) {
                assertTrue("cheap leftover must not precede a 32-leaf group", sawFullFileCapGroup);
            }
        }
        assertTrue("expected at least one group at the file cap", sawFullFileCapGroup);
    }

    public void testInvalidMaxFilesPerGroupThrows() {
        List<ExternalSplit> splits = makeSplits(COALESCING_THRESHOLD + 1);
        expectThrows(IllegalArgumentException.class, () -> SplitCoalescer.coalesce(splits, 128 * 1024 * 1024, 8, 1, 0));
    }

    public void testMixedSizesProducesReasonableGroups() {
        List<ExternalSplit> splits = new ArrayList<>();
        long targetGroupSize = 100 * 1024 * 1024;
        for (int i = 0; i < 50; i++) {
            long size = randomLongBetween(1024, 50 * 1024 * 1024);
            splits.add(makeFileSplit(i, size));
        }

        List<ExternalSplit> result = SplitCoalescer.coalesce(splits, targetGroupSize, 8);

        assertEquals(50, countTotalLeaves(result));
        assertTrue("Should produce fewer groups", result.size() < 50);
    }

    private static List<ExternalSplit> makeSplits(int count) {
        return makeSplits(count, 1024);
    }

    private static List<ExternalSplit> makeSplits(int count, long fileSize) {
        List<ExternalSplit> splits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            splits.add(makeFileSplit(i, fileSize));
        }
        return splits;
    }

    private static List<ExternalSplit> makeSplitsWithoutSize(int count) {
        List<ExternalSplit> splits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            splits.add(new UnknownSizeSplit("file"));
        }
        return splits;
    }

    private static class UnknownSizeSplit implements ExternalSplit {
        private final String sourceType;

        UnknownSizeSplit(String sourceType) {
            this.sourceType = sourceType;
        }

        @Override
        public String sourceType() {
            return sourceType;
        }

        @Override
        public String getWriteableName() {
            return "unknown";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    private static FileSplit makeFileSplit(int index, long length) {
        return new FileSplit(
            "file",
            StoragePath.of("s3://bucket/data/file" + index + ".parquet"),
            0,
            length,
            ".parquet",
            Map.of(),
            Map.of()
        );
    }

    private static int countTotalLeaves(List<ExternalSplit> splits) {
        int count = 0;
        for (ExternalSplit split : splits) {
            count += leafCount(split);
        }
        return count;
    }

    private static int maxLeaves(List<ExternalSplit> groups) {
        int max = 0;
        for (ExternalSplit group : groups) {
            max = Math.max(max, leafCount(group));
        }
        return max;
    }

    private static int leafCount(ExternalSplit split) {
        return split instanceof CoalescedSplit coalesced ? coalesced.children().size() : 1;
    }

    private static long claimCost(ExternalSplit split) {
        return Math.max(0L, split.estimatedSizeInBytes()) + (long) leafCount(split) * DEFAULT_OPEN_COST_BYTES;
    }
}
