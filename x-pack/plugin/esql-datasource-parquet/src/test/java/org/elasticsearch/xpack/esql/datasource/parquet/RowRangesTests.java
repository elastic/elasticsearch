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

import static org.hamcrest.Matchers.equalTo;

public class RowRangesTests extends ESTestCase {

    public void testAllCoversEntireRowGroup() {
        RowRanges all = RowRanges.all(1000);
        assertThat(all.selectedRowCount(), equalTo(1000L));
        assertThat(all.density(), equalTo(1.0));
        assertThat(all.rangeCount(), equalTo(1));
        assertTrue(all.isAll());
        assertFalse(all.isEmpty());
    }

    public void testEmptyRowGroup() {
        RowRanges empty = RowRanges.all(0);
        assertThat(empty.selectedRowCount(), equalTo(0L));
        assertTrue(empty.isEmpty());
    }

    public void testSingleRange() {
        RowRanges range = RowRanges.of(100, 500, 1000);
        assertThat(range.selectedRowCount(), equalTo(400L));
        assertThat(range.density(), equalTo(0.4));
        assertThat(range.rangeCount(), equalTo(1));
        assertThat(range.rangeStart(0), equalTo(100L));
        assertThat(range.rangeEnd(0), equalTo(500L));
        assertFalse(range.isAll());
        assertFalse(range.isEmpty());
    }

    public void testEmptyRange() {
        RowRanges range = RowRanges.of(500, 500, 1000);
        assertTrue(range.isEmpty());
        assertThat(range.selectedRowCount(), equalTo(0L));
    }

    public void testIntersectOverlapping() {
        RowRanges a = RowRanges.of(0, 500, 1000);
        RowRanges b = RowRanges.of(300, 800, 1000);
        RowRanges result = a.intersect(b);
        assertThat(result.rangeCount(), equalTo(1));
        assertThat(result.rangeStart(0), equalTo(300L));
        assertThat(result.rangeEnd(0), equalTo(500L));
        assertThat(result.selectedRowCount(), equalTo(200L));
    }

    public void testIntersectNoOverlap() {
        RowRanges a = RowRanges.of(0, 100, 1000);
        RowRanges b = RowRanges.of(200, 300, 1000);
        RowRanges result = a.intersect(b);
        assertTrue(result.isEmpty());
    }

    public void testIntersectMultipleRanges() {
        RowRanges a = RowRanges.ofSorted(new long[] { 0, 200, 500 }, new long[] { 100, 400, 700 }, 1000);
        RowRanges b = RowRanges.ofSorted(new long[] { 50, 350 }, new long[] { 250, 600 }, 1000);
        RowRanges result = a.intersect(b);
        assertThat(result.rangeCount(), equalTo(4));
        assertThat(result.rangeStart(0), equalTo(50L));
        assertThat(result.rangeEnd(0), equalTo(100L));
        assertThat(result.rangeStart(1), equalTo(200L));
        assertThat(result.rangeEnd(1), equalTo(250L));
        assertThat(result.rangeStart(2), equalTo(350L));
        assertThat(result.rangeEnd(2), equalTo(400L));
        assertThat(result.rangeStart(3), equalTo(500L));
        assertThat(result.rangeEnd(3), equalTo(600L));
    }

    public void testIntersectWithEmpty() {
        RowRanges a = RowRanges.of(0, 500, 1000);
        RowRanges empty = RowRanges.of(0, 0, 1000);
        assertTrue(a.intersect(empty).isEmpty());
        assertTrue(empty.intersect(a).isEmpty());
    }

    public void testUnionNonOverlapping() {
        RowRanges a = RowRanges.of(0, 100, 1000);
        RowRanges b = RowRanges.of(200, 300, 1000);
        RowRanges result = a.union(b);
        assertThat(result.rangeCount(), equalTo(2));
        assertThat(result.selectedRowCount(), equalTo(200L));
    }

    public void testUnionOverlapping() {
        RowRanges a = RowRanges.of(0, 200, 1000);
        RowRanges b = RowRanges.of(100, 300, 1000);
        RowRanges result = a.union(b);
        assertThat(result.rangeCount(), equalTo(1));
        assertThat(result.rangeStart(0), equalTo(0L));
        assertThat(result.rangeEnd(0), equalTo(300L));
        assertThat(result.selectedRowCount(), equalTo(300L));
    }

    public void testUnionWithEmpty() {
        RowRanges a = RowRanges.of(0, 500, 1000);
        RowRanges empty = RowRanges.of(0, 0, 1000);
        RowRanges result = a.union(empty);
        assertThat(result.selectedRowCount(), equalTo(500L));
    }

    public void testComplement() {
        RowRanges range = RowRanges.of(200, 800, 1000);
        RowRanges complement = range.complement();
        assertThat(complement.rangeCount(), equalTo(2));
        assertThat(complement.rangeStart(0), equalTo(0L));
        assertThat(complement.rangeEnd(0), equalTo(200L));
        assertThat(complement.rangeStart(1), equalTo(800L));
        assertThat(complement.rangeEnd(1), equalTo(1000L));
        assertThat(complement.selectedRowCount(), equalTo(400L));
    }

    public void testComplementOfAll() {
        RowRanges all = RowRanges.all(1000);
        RowRanges complement = all.complement();
        assertTrue(complement.isEmpty());
    }

    public void testComplementOfEmpty() {
        RowRanges empty = RowRanges.of(0, 0, 1000);
        RowRanges complement = empty.complement();
        assertTrue(complement.isAll());
        assertThat(complement.selectedRowCount(), equalTo(1000L));
    }

    public void testComplementMultipleRanges() {
        RowRanges ranges = RowRanges.ofSorted(new long[] { 100, 500 }, new long[] { 200, 600 }, 1000);
        RowRanges complement = ranges.complement();
        assertThat(complement.rangeCount(), equalTo(3));
        assertThat(complement.rangeStart(0), equalTo(0L));
        assertThat(complement.rangeEnd(0), equalTo(100L));
        assertThat(complement.rangeStart(1), equalTo(200L));
        assertThat(complement.rangeEnd(1), equalTo(500L));
        assertThat(complement.rangeStart(2), equalTo(600L));
        assertThat(complement.rangeEnd(2), equalTo(1000L));
    }

    public void testDensity() {
        RowRanges range = RowRanges.of(0, 800, 1000);
        assertThat(range.density(), equalTo(0.8));
    }

    public void testDensityZeroTotal() {
        RowRanges range = RowRanges.of(0, 0, 0);
        assertThat(range.density(), equalTo(1.0));
    }

    public void testAntiFragmentationHighDensity() {
        RowRanges range = RowRanges.of(0, 900, 1000);
        assertTrue(range.shouldDiscard(0.8, 32));
    }

    public void testAntiFragmentationLowDensity() {
        RowRanges range = RowRanges.of(0, 200, 1000);
        assertFalse(range.shouldDiscard(0.8, 32));
    }

    public void testAntiFragmentationTooManyTransitions() {
        long[] starts = new long[40];
        long[] ends = new long[40];
        for (int i = 0; i < 40; i++) {
            starts[i] = i * 25;
            ends[i] = i * 25 + 10;
        }
        RowRanges fragmented = RowRanges.ofSorted(starts, ends, 1000);
        assertTrue(fragmented.shouldDiscard(0.8, 32));
    }

    public void testAntiFragmentationFewTransitions() {
        RowRanges ranges = RowRanges.ofSorted(new long[] { 0, 500 }, new long[] { 100, 600 }, 1000);
        assertFalse(ranges.shouldDiscard(0.8, 32));
    }

    public void testFromUnsortedMergesOverlapping() {
        List<long[]> ranges = new ArrayList<>();
        ranges.add(new long[] { 200, 400 });
        ranges.add(new long[] { 0, 100 });
        ranges.add(new long[] { 50, 250 });
        RowRanges result = RowRanges.fromUnsorted(ranges, 1000);
        assertThat(result.rangeCount(), equalTo(1));
        assertThat(result.rangeStart(0), equalTo(0L));
        assertThat(result.rangeEnd(0), equalTo(400L));
    }

    public void testFromUnsortedNonOverlapping() {
        List<long[]> ranges = new ArrayList<>();
        ranges.add(new long[] { 500, 600 });
        ranges.add(new long[] { 0, 100 });
        ranges.add(new long[] { 200, 300 });
        RowRanges result = RowRanges.fromUnsorted(ranges, 1000);
        assertThat(result.rangeCount(), equalTo(3));
        assertThat(result.rangeStart(0), equalTo(0L));
        assertThat(result.rangeEnd(0), equalTo(100L));
        assertThat(result.rangeStart(1), equalTo(200L));
        assertThat(result.rangeEnd(1), equalTo(300L));
        assertThat(result.rangeStart(2), equalTo(500L));
        assertThat(result.rangeEnd(2), equalTo(600L));
    }

    public void testFromUnsortedEmpty() {
        RowRanges result = RowRanges.fromUnsorted(new ArrayList<>(), 1000);
        assertTrue(result.isEmpty());
    }

    public void testIntersectIdentity() {
        RowRanges a = RowRanges.of(100, 500, 1000);
        RowRanges result = a.intersect(RowRanges.all(1000));
        assertThat(result.rangeCount(), equalTo(1));
        assertThat(result.rangeStart(0), equalTo(100L));
        assertThat(result.rangeEnd(0), equalTo(500L));
    }

    public void testToString() {
        RowRanges range = RowRanges.of(100, 500, 1000);
        String str = range.toString();
        assertTrue(str.contains("[100, 500)"));
        assertTrue(str.contains("total=1000"));
    }

    public void testOverlapsEmpty() {
        RowRanges empty = RowRanges.of(0, 0, 1000);
        assertFalse(empty.overlaps(0, 100));
    }

    public void testOverlapsSingleRangeFullyInsideSelection() {
        RowRanges range = RowRanges.of(100, 500, 1000);
        assertTrue(range.overlaps(200, 300));
    }

    public void testOverlapsSingleRangePartialOverlapAtStart() {
        RowRanges range = RowRanges.of(100, 200, 1000);
        assertTrue(range.overlaps(50, 150));
    }

    public void testOverlapsSingleRangePartialOverlapAtEnd() {
        RowRanges range = RowRanges.of(100, 200, 1000);
        assertTrue(range.overlaps(150, 250));
    }

    public void testOverlapsSingleRangeNoOverlapBefore() {
        RowRanges range = RowRanges.of(100, 200, 1000);
        assertFalse(range.overlaps(0, 50));
    }

    public void testOverlapsSingleRangeNoOverlapAfter() {
        RowRanges range = RowRanges.of(100, 200, 1000);
        assertFalse(range.overlaps(300, 400));
    }

    public void testOverlapsMultipleRangesFirst() {
        RowRanges ranges = RowRanges.ofSorted(new long[] { 0, 500 }, new long[] { 100, 600 }, 1000);
        assertTrue(ranges.overlaps(50, 80));
    }

    public void testOverlapsMultipleRangesMiddle() {
        RowRanges ranges = RowRanges.ofSorted(new long[] { 0, 200, 500 }, new long[] { 100, 300, 700 }, 1000);
        assertTrue(ranges.overlaps(250, 280));
    }

    public void testOverlapsMultipleRangesGap() {
        RowRanges ranges = RowRanges.ofSorted(new long[] { 0, 200 }, new long[] { 100, 300 }, 1000);
        assertFalse(ranges.overlaps(120, 180));
    }

    public void testOverlapsBoundaryPageStartEqualsRangeEnd() {
        RowRanges range = RowRanges.of(100, 200, 1000);
        assertFalse(range.overlaps(200, 300));
    }

    public void testOverlapsBoundaryPageEndEqualsRangeStart() {
        RowRanges range = RowRanges.of(100, 200, 1000);
        assertFalse(range.overlaps(0, 100));
    }

    public void testOverlapsBoundaryPageStartEqualsRangeStart() {
        RowRanges range = RowRanges.of(100, 200, 1000);
        assertTrue(range.overlaps(100, 150));
    }

    public void testFirstSelectedInRangeEmpty() {
        RowRanges empty = RowRanges.of(0, 0, 1000);
        assertThat(empty.firstSelectedInRange(0, 100), equalTo(-1L));
    }

    public void testFirstSelectedInRangeFullyInsideSelection() {
        RowRanges range = RowRanges.of(100, 500, 1000);
        assertThat(range.firstSelectedInRange(200, 400), equalTo(200L));
    }

    public void testFirstSelectedInRangeStartsBeforeSelection() {
        RowRanges range = RowRanges.of(100, 500, 1000);
        assertThat(range.firstSelectedInRange(50, 200), equalTo(100L));
    }

    public void testFirstSelectedInRangeStartsInMiddleOfSelection() {
        RowRanges range = RowRanges.of(100, 500, 1000);
        assertThat(range.firstSelectedInRange(150, 200), equalTo(150L));
    }

    public void testFirstSelectedInRangeNoSelectionInRange() {
        RowRanges range = RowRanges.of(100, 500, 1000);
        assertThat(range.firstSelectedInRange(0, 50), equalTo(-1L));
    }

    public void testFirstSelectedInRangeMultipleRangesSecondRange() {
        RowRanges ranges = RowRanges.ofSorted(new long[] { 0, 200 }, new long[] { 100, 350 }, 1000);
        assertThat(ranges.firstSelectedInRange(250, 400), equalTo(250L));
    }

    public void testLastSelectedInRangeEmpty() {
        RowRanges empty = RowRanges.of(0, 0, 1000);
        assertThat(empty.lastSelectedInRange(0, 100), equalTo(-1L));
    }

    public void testLastSelectedInRangeFullyInsideSelection() {
        RowRanges range = RowRanges.of(100, 500, 1000);
        assertThat(range.lastSelectedInRange(200, 400), equalTo(399L));
    }

    public void testLastSelectedInRangeEndsAfterSelection() {
        RowRanges range = RowRanges.of(100, 300, 1000);
        assertThat(range.lastSelectedInRange(50, 500), equalTo(299L));
    }

    public void testLastSelectedInRangeEndsInMiddleOfSelection() {
        RowRanges range = RowRanges.of(100, 500, 1000);
        assertThat(range.lastSelectedInRange(50, 250), equalTo(249L));
    }

    public void testLastSelectedInRangeNoSelectionInRange() {
        RowRanges range = RowRanges.of(100, 500, 1000);
        assertThat(range.lastSelectedInRange(0, 50), equalTo(-1L));
    }

    public void testLastSelectedInRangeMultipleRanges() {
        RowRanges ranges = RowRanges.ofSorted(new long[] { 0, 150 }, new long[] { 100, 300 }, 1000);
        assertThat(ranges.lastSelectedInRange(50, 250), equalTo(249L));
    }

    public void testNextRunInPageEmpty() {
        RowRanges empty = RowRanges.of(0, 0, 1000);
        assertThat(empty.nextRunInPage(0, 128), equalTo(new RowRanges.Run(128, 0)));
    }

    public void testNextRunInPageFullySelected() {
        RowRanges all = RowRanges.all(1000);
        assertThat(all.nextRunInPage(0, 64), equalTo(new RowRanges.Run(0, 64)));
    }

    public void testNextRunInPageLeadingGap() {
        RowRanges range = RowRanges.of(5, 100, 1000);
        assertThat(range.nextRunInPage(0, 50), equalTo(new RowRanges.Run(5, 45)));
    }

    public void testNextRunInPageNoSelectionInWindow() {
        RowRanges range = RowRanges.of(100, 200, 1000);
        assertThat(range.nextRunInPage(0, 50), equalTo(new RowRanges.Run(50, 0)));
    }

    public void testNextRunInPageTruncatedToWindow() {
        RowRanges range = RowRanges.of(0, 100, 1000);
        assertThat(range.nextRunInPage(90, 20), equalTo(new RowRanges.Run(0, 10)));
    }
}
