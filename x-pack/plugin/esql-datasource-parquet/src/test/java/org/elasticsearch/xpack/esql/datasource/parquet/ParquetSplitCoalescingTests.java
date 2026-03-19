/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

public class ParquetSplitCoalescingTests extends ESTestCase {

    public void testCoalesceProducesFewerRangesAndPreservesCoverage() {
        List<long[]> ranges = new ArrayList<>();
        ranges.add(new long[] { 0, 10 });
        ranges.add(new long[] { 10, 10 });
        ranges.add(new long[] { 20, 10 });
        ranges.add(new long[] { 30, 10 });
        ranges.add(new long[] { 40, 10 });

        List<long[]> coalesced = ParquetFormatReader.coalesceRowGroupRanges(ranges, 25);
        assertTrue(coalesced.size() < ranges.size());

        for (long[] original : ranges) {
            long start = original[0];
            assertTrue("Missing coverage for start=" + start, isCoveredByAny(coalesced, start));
        }

        assertSortedNonOverlapping(coalesced);
    }

    public void testCoalesceWithLargeTargetProducesSingleRange() {
        List<long[]> ranges = List.of(new long[] { 0, 10 }, new long[] { 10, 10 }, new long[] { 20, 10 });
        List<long[]> coalesced = ParquetFormatReader.coalesceRowGroupRanges(ranges, 1024 * 1024);
        assertEquals(1, coalesced.size());
        assertEquals(0, coalesced.get(0)[0]);
        assertEquals(30, coalesced.get(0)[1]);
        assertSortedNonOverlapping(coalesced);
    }

    public void testCoalesceWithNonPositiveTargetReturnsOriginal() {
        List<long[]> ranges = List.of(new long[] { 20, 10 }, new long[] { 0, 10 }, new long[] { 10, 10 });
        List<long[]> coalesced = ParquetFormatReader.coalesceRowGroupRanges(ranges, 0);
        assertEquals(ranges.size(), coalesced.size());
        for (int i = 0; i < ranges.size(); i++) {
            assertArrayEquals(ranges.get(i), coalesced.get(i));
        }
    }

    private static boolean isCoveredByAny(List<long[]> coalesced, long start) {
        for (long[] group : coalesced) {
            long groupStart = group[0];
            long groupEnd = groupStart + group[1];
            if (start >= groupStart && start < groupEnd) {
                return true;
            }
        }
        return false;
    }

    private static void assertSortedNonOverlapping(List<long[]> ranges) {
        long lastEnd = Long.MIN_VALUE;
        for (long[] r : ranges) {
            assertTrue("length must be > 0", r[1] > 0);
            assertTrue("ranges must be sorted and non-overlapping", r[0] >= lastEnd);
            lastEnd = r[0] + r[1];
        }
    }
}
