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

public class WordMaskRowRangesConverterTests extends ESTestCase {

    public void testEmptyMaskProducesEmptyRanges() {
        WordMask mask = new WordMask();
        mask.reset(100);
        RowRanges ranges = WordMaskRowRangesConverter.fromWordMask(mask, 100);
        assertTrue(ranges.isEmpty());
        assertThat(ranges.totalRows(), equalTo(100L));
    }

    public void testZeroSizeRowGroupProducesAll() {
        WordMask mask = new WordMask();
        mask.reset(0);
        RowRanges ranges = WordMaskRowRangesConverter.fromWordMask(mask, 0);
        assertTrue(ranges.isEmpty());
    }

    public void testSingleRunWithinFirstWord() {
        WordMask mask = new WordMask();
        mask.reset(64);
        for (int i = 5; i < 12; i++) {
            mask.set(i);
        }
        RowRanges ranges = WordMaskRowRangesConverter.fromWordMask(mask, 64);
        assertThat(ranges.rangeCount(), equalTo(1));
        assertThat(ranges.rangeStart(0), equalTo(5L));
        assertThat(ranges.rangeEnd(0), equalTo(12L));
        assertThat(ranges.selectedRowCount(), equalTo(7L));
    }

    public void testTwoSeparateRuns() {
        WordMask mask = new WordMask();
        mask.reset(64);
        for (int i : new int[] { 1, 2, 3 }) {
            mask.set(i);
        }
        for (int i : new int[] { 30, 31 }) {
            mask.set(i);
        }
        RowRanges ranges = WordMaskRowRangesConverter.fromWordMask(mask, 64);
        assertThat(ranges.rangeCount(), equalTo(2));
        assertThat(ranges.rangeStart(0), equalTo(1L));
        assertThat(ranges.rangeEnd(0), equalTo(4L));
        assertThat(ranges.rangeStart(1), equalTo(30L));
        assertThat(ranges.rangeEnd(1), equalTo(32L));
    }

    public void testRunSpanningWordBoundary() {
        WordMask mask = new WordMask();
        mask.reset(200);
        // Set bits 60-70, spanning the boundary between word 0 and word 1.
        for (int i = 60; i < 70; i++) {
            mask.set(i);
        }
        RowRanges ranges = WordMaskRowRangesConverter.fromWordMask(mask, 200);
        assertThat(ranges.rangeCount(), equalTo(1));
        assertThat(ranges.rangeStart(0), equalTo(60L));
        assertThat(ranges.rangeEnd(0), equalTo(70L));
    }

    public void testAllBitsSetProducesSingleAllRange() {
        WordMask mask = new WordMask();
        mask.setAll(150);
        RowRanges ranges = WordMaskRowRangesConverter.fromWordMask(mask, 150);
        assertThat(ranges.rangeCount(), equalTo(1));
        assertThat(ranges.rangeStart(0), equalTo(0L));
        assertThat(ranges.rangeEnd(0), equalTo(150L));
        assertTrue(ranges.isAll());
    }

    public void testTrailingBitsBeyondTotalRowsAreIgnored() {
        // Create a 70-bit logical mask but underlying long array has 64-bit words; bits 65..71
        // beyond totalRows must not become ranges.
        WordMask mask = new WordMask();
        mask.reset(70);
        for (int i = 60; i < 64; i++) {
            mask.set(i);
        }
        // Position 65 is within the logical mask; positions 70+ are not.
        mask.set(65);
        // Manually set bits 70..71 inside the trailing word (out-of-range for the converter).
        mask.set(70);
        mask.set(71);
        RowRanges ranges = WordMaskRowRangesConverter.fromWordMask(mask, 70);
        // Expect runs [60,64), [65,66) — bits 70..71 must be ignored even though they were set
        // in the underlying word. Bits 70..71 lie at totalRows or beyond and must not extend
        // the second run.
        assertThat(ranges.rangeCount(), equalTo(2));
        assertThat(ranges.rangeStart(0), equalTo(60L));
        assertThat(ranges.rangeEnd(0), equalTo(64L));
        assertThat(ranges.rangeStart(1), equalTo(65L));
        assertThat(ranges.rangeEnd(1), equalTo(66L));
    }

    public void testSparseMaskProducesManyRanges() {
        WordMask mask = new WordMask();
        mask.reset(20);
        // Every other bit: positions 0, 2, 4, ..., 18.
        int expected = 0;
        for (int i = 0; i < 20; i += 2) {
            mask.set(i);
            expected++;
        }
        RowRanges ranges = WordMaskRowRangesConverter.fromWordMask(mask, 20);
        assertThat(ranges.rangeCount(), equalTo(expected));
        long total = 0;
        for (int i = 0; i < ranges.rangeCount(); i++) {
            assertThat(ranges.rangeEnd(i) - ranges.rangeStart(i), equalTo(1L));
            total += ranges.rangeEnd(i) - ranges.rangeStart(i);
        }
        assertThat(total, equalTo((long) expected));
    }

    public void testRandomizedRoundTripAgainstSurvivingPositions() {
        // Build a random mask, derive ranges, then verify each surviving position lies in some
        // range and each in-range position has its bit set.
        int totalRows = randomIntBetween(1, 5000);
        WordMask mask = new WordMask();
        mask.reset(totalRows);
        List<Integer> set = new ArrayList<>();
        for (int i = 0; i < totalRows; i++) {
            if (randomBoolean()) {
                mask.set(i);
                set.add(i);
            }
        }
        RowRanges ranges = WordMaskRowRangesConverter.fromWordMask(mask, totalRows);
        // Every set position should be inside one of the ranges.
        for (int p : set) {
            boolean found = false;
            for (int r = 0; r < ranges.rangeCount(); r++) {
                if (ranges.rangeStart(r) <= p && p < ranges.rangeEnd(r)) {
                    found = true;
                    break;
                }
            }
            assertTrue("position " + p + " should be in ranges " + ranges, found);
        }
        // Every in-range position should have its bit set (no false positives).
        for (int r = 0; r < ranges.rangeCount(); r++) {
            for (long p = ranges.rangeStart(r); p < ranges.rangeEnd(r); p++) {
                assertTrue("position " + p + " in range but bit not set", mask.get((int) p));
            }
        }
        // Selected count should equal the number of set bits.
        assertThat(ranges.selectedRowCount(), equalTo((long) set.size()));
    }

    public void testRangesAreContiguousAndNonOverlapping() {
        // A descriptive sanity check: produced ranges must be sorted, non-overlapping, and have
        // strictly positive length.
        int totalRows = 1000;
        WordMask mask = new WordMask();
        mask.reset(totalRows);
        for (int i = 0; i < totalRows; i++) {
            if (random().nextDouble() < 0.3) {
                mask.set(i);
            }
        }
        RowRanges ranges = WordMaskRowRangesConverter.fromWordMask(mask, totalRows);
        long prevEnd = -1;
        for (int r = 0; r < ranges.rangeCount(); r++) {
            assertTrue("range " + r + " has non-positive length", ranges.rangeEnd(r) > ranges.rangeStart(r));
            assertTrue("range " + r + " overlaps or precedes previous", ranges.rangeStart(r) > prevEnd);
            prevEnd = ranges.rangeEnd(r);
        }
    }
}
