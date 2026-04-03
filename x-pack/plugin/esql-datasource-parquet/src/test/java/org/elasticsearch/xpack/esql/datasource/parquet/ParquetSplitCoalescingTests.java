/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParquetSplitCoalescingTests extends ESTestCase {

    public void testCoalesceProducesFewerRangesAndPreservesCoverage() {
        List<SplitRange> ranges = new ArrayList<>();
        ranges.add(new SplitRange(0, 10, Map.of("_stats.row_count", 10L)));
        ranges.add(new SplitRange(10, 10, Map.of("_stats.row_count", 10L)));
        ranges.add(new SplitRange(20, 10, Map.of("_stats.row_count", 10L)));
        ranges.add(new SplitRange(30, 10, Map.of("_stats.row_count", 10L)));
        ranges.add(new SplitRange(40, 10, Map.of("_stats.row_count", 10L)));

        List<SplitRange> coalesced = ParquetFormatReader.coalesceRowGroupRanges(ranges, 25);
        assertTrue(coalesced.size() < ranges.size());

        for (SplitRange original : ranges) {
            long start = original.offset();
            assertTrue("Missing coverage for start=" + start, isCoveredByAny(coalesced, start));
        }

        assertSortedNonOverlapping(coalesced);
    }

    public void testCoalesceWithLargeTargetProducesSingleRange() {
        List<SplitRange> ranges = List.of(
            new SplitRange(0, 10, Map.of("_stats.row_count", 5L)),
            new SplitRange(10, 10, Map.of("_stats.row_count", 5L)),
            new SplitRange(20, 10, Map.of("_stats.row_count", 5L))
        );
        List<SplitRange> coalesced = ParquetFormatReader.coalesceRowGroupRanges(ranges, 1024 * 1024);
        assertEquals(1, coalesced.size());
        assertEquals(0, coalesced.get(0).offset());
        assertEquals(30, coalesced.get(0).length());
        assertEquals(15L, coalesced.get(0).statistics().get("_stats.row_count"));
        assertSortedNonOverlapping(coalesced);
    }

    public void testCoalesceWithNonPositiveTargetReturnsOriginal() {
        List<SplitRange> ranges = List.of(new SplitRange(20, 10), new SplitRange(0, 10), new SplitRange(10, 10));
        List<SplitRange> coalesced = ParquetFormatReader.coalesceRowGroupRanges(ranges, 0);
        assertEquals(ranges.size(), coalesced.size());
        for (int i = 0; i < ranges.size(); i++) {
            assertEquals(ranges.get(i).offset(), coalesced.get(i).offset());
            assertEquals(ranges.get(i).length(), coalesced.get(i).length());
        }
    }

    private static boolean isCoveredByAny(List<SplitRange> coalesced, long start) {
        for (SplitRange group : coalesced) {
            long groupStart = group.offset();
            long groupEnd = groupStart + group.length();
            if (start >= groupStart && start < groupEnd) {
                return true;
            }
        }
        return false;
    }

    private static void assertSortedNonOverlapping(List<SplitRange> ranges) {
        long lastEnd = Long.MIN_VALUE;
        for (SplitRange r : ranges) {
            assertTrue("length must be > 0", r.length() > 0);
            assertTrue("ranges must be sorted and non-overlapping", r.offset() >= lastEnd);
            lastEnd = r.offset() + r.length();
        }
    }
}
