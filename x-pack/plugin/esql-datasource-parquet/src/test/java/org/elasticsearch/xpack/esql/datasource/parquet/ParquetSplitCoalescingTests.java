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

import static org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.STATS_ROW_COUNT;

public class ParquetSplitCoalescingTests extends ESTestCase {

    public void testCoalesceProducesFewerRangesAndPreservesCoverage() {
        List<SplitRange> ranges = new ArrayList<>();
        ranges.add(range(0, 10, 10));
        ranges.add(range(10, 10, 10));
        ranges.add(range(20, 10, 10));
        ranges.add(range(30, 10, 10));
        ranges.add(range(40, 10, 10));

        List<SplitRange> coalesced = ParquetFormatReader.coalesceRowGroupRanges(ranges, 25);
        assertTrue(coalesced.size() < ranges.size());

        for (SplitRange original : ranges) {
            long start = original.offset();
            assertTrue("Missing coverage for start=" + start, isCoveredByAny(coalesced, start));
        }

        assertSortedNonOverlapping(coalesced);
    }

    public void testCoalesceWithLargeTargetProducesSingleRange() {
        List<SplitRange> ranges = List.of(range(0, 10, 5), range(10, 10, 5), range(20, 10, 5));
        List<SplitRange> coalesced = ParquetFormatReader.coalesceRowGroupRanges(ranges, 1024 * 1024);
        assertEquals(1, coalesced.size());
        SplitRange only = coalesced.get(0);
        assertEquals(0, only.offset());
        assertEquals(30, only.length());
        assertEquals(15L, rowCount(only));
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

    public void testCoalesceLargeRangeNotAbsorbedByAccumulator() {
        List<SplitRange> ranges = List.of(range(0, 10, 5), range(10, 10, 5), range(20, 30, 50));
        List<SplitRange> coalesced = ParquetFormatReader.coalesceRowGroupRanges(ranges, 25);
        assertEquals(2, coalesced.size());

        SplitRange smalls = coalesced.get(0);
        assertEquals(0, smalls.offset());
        assertEquals(20, smalls.length());
        assertEquals(10L, rowCount(smalls));

        SplitRange large = coalesced.get(1);
        assertEquals(20, large.offset());
        assertEquals(30, large.length());
        assertEquals(50L, rowCount(large));

        assertSortedNonOverlapping(coalesced);
    }

    public void testCoalesceAllLargeRangesStandAlone() {
        List<SplitRange> ranges = List.of(range(0, 30, 10), range(30, 40, 20), range(70, 25, 15));
        List<SplitRange> coalesced = ParquetFormatReader.coalesceRowGroupRanges(ranges, 25);
        assertEquals(3, coalesced.size());
        for (int i = 0; i < ranges.size(); i++) {
            assertEquals(ranges.get(i).offset(), coalesced.get(i).offset());
            assertEquals(ranges.get(i).length(), coalesced.get(i).length());
        }
        assertSortedNonOverlapping(coalesced);
    }

    public void testCoalesceFirstRangeLargeFollowedBySmalls() {
        List<SplitRange> ranges = List.of(range(0, 50, 30), range(50, 5, 2), range(55, 5, 2));
        List<SplitRange> coalesced = ParquetFormatReader.coalesceRowGroupRanges(ranges, 25);
        assertEquals(2, coalesced.size());

        SplitRange large = coalesced.get(0);
        assertEquals(0, large.offset());
        assertEquals(50, large.length());
        assertEquals(30L, rowCount(large));

        SplitRange tail = coalesced.get(1);
        assertEquals(50, tail.offset());
        assertEquals(10, tail.length());
        assertEquals(4L, rowCount(tail));

        assertSortedNonOverlapping(coalesced);
    }

    private static SplitRange range(long offset, long length, long rowCount) {
        return new SplitRange(offset, length, Map.of(STATS_ROW_COUNT, rowCount));
    }

    private static long rowCount(SplitRange range) {
        return (long) range.statistics().get(STATS_ROW_COUNT);
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
