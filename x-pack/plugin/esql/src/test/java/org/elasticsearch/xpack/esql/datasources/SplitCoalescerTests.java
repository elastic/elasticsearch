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
        assertSame(splits, SplitCoalescer.coalesce(splits));
    }

    public void testExactlyAtThresholdReturnsUnchanged() {
        List<ExternalSplit> splits = makeSplits(COALESCING_THRESHOLD);
        assertSame(splits, SplitCoalescer.coalesce(splits));
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
            if (split instanceof CoalescedSplit coalesced) {
                count += coalesced.children().size();
            } else {
                count++;
            }
        }
        return count;
    }
}
