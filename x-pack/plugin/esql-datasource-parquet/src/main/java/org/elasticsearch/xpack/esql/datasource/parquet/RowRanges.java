/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a set of selected row ranges within a row group as sorted, non-overlapping
 * {@code [start, end)} intervals. Used for page-level skipping: after evaluating ColumnIndex
 * min/max per data page, only pages whose row ranges intersect with the selected ranges
 * need to be decoded.
 *
 * <p>Supports set operations (intersect, union) across predicate columns that may have
 * different page boundaries, plus an anti-fragmentation check to avoid excessive range
 * transitions that would negate the benefit of page skipping.
 *
 * <p>Inspired by Trino's {@code FilteredRowRangesIterator}, independently implemented.
 */
final class RowRanges {

    static final double DEFAULT_DENSITY_THRESHOLD = 0.8;
    static final int DEFAULT_MAX_TRANSITIONS = 32;

    private final long[] starts;
    private final long[] ends;
    private final long totalRows;

    private RowRanges(long[] starts, long[] ends, long totalRows) {
        assert starts.length == ends.length;
        this.starts = starts;
        this.ends = ends;
        this.totalRows = totalRows;
    }

    /**
     * Creates a RowRanges covering all rows in the row group (no skipping).
     */
    static RowRanges all(long rowCount) {
        if (rowCount <= 0) {
            return new RowRanges(new long[0], new long[0], 0);
        }
        return new RowRanges(new long[] { 0 }, new long[] { rowCount }, rowCount);
    }

    /**
     * Creates a RowRanges from a single range.
     */
    static RowRanges of(long start, long end, long totalRows) {
        if (start >= end) {
            return new RowRanges(new long[0], new long[0], totalRows);
        }
        return new RowRanges(new long[] { start }, new long[] { end }, totalRows);
    }

    /**
     * Creates a RowRanges from a list of non-overlapping, sorted ranges.
     * The caller must ensure ranges are sorted by start and non-overlapping.
     */
    static RowRanges ofSorted(long[] starts, long[] ends, long totalRows) {
        return new RowRanges(starts.clone(), ends.clone(), totalRows);
    }

    /**
     * Creates a RowRanges by merging a list of potentially overlapping ranges.
     * Ranges are sorted and merged into non-overlapping intervals.
     */
    static RowRanges fromUnsorted(List<long[]> ranges, long totalRows) {
        if (ranges.isEmpty()) {
            return new RowRanges(new long[0], new long[0], totalRows);
        }
        ranges.sort((a, b) -> Long.compare(a[0], b[0]));

        List<long[]> merged = new ArrayList<>();
        long[] current = ranges.get(0);
        for (int i = 1; i < ranges.size(); i++) {
            long[] next = ranges.get(i);
            if (next[0] <= current[1]) {
                current = new long[] { current[0], Math.max(current[1], next[1]) };
            } else {
                merged.add(current);
                current = next;
            }
        }
        merged.add(current);

        long[] starts = new long[merged.size()];
        long[] ends = new long[merged.size()];
        for (int i = 0; i < merged.size(); i++) {
            starts[i] = merged.get(i)[0];
            ends[i] = merged.get(i)[1];
        }
        return new RowRanges(starts, ends, totalRows);
    }

    /**
     * Intersects this RowRanges with another, producing ranges that are in both sets.
     * Both inputs must be sorted and non-overlapping.
     */
    RowRanges intersect(RowRanges other) {
        if (isEmpty() || other.isEmpty()) {
            return new RowRanges(new long[0], new long[0], totalRows);
        }

        List<long[]> result = new ArrayList<>();
        int i = 0, j = 0;
        while (i < starts.length && j < other.starts.length) {
            long overlapStart = Math.max(starts[i], other.starts[j]);
            long overlapEnd = Math.min(ends[i], other.ends[j]);
            if (overlapStart < overlapEnd) {
                result.add(new long[] { overlapStart, overlapEnd });
            }
            if (ends[i] < other.ends[j]) {
                i++;
            } else {
                j++;
            }
        }

        long[] rs = new long[result.size()];
        long[] re = new long[result.size()];
        for (int k = 0; k < result.size(); k++) {
            rs[k] = result.get(k)[0];
            re[k] = result.get(k)[1];
        }
        return new RowRanges(rs, re, totalRows);
    }

    /**
     * Unions this RowRanges with another, producing ranges that are in either set.
     * Both inputs must be sorted and non-overlapping.
     */
    RowRanges union(RowRanges other) {
        if (isEmpty()) {
            return other;
        }
        if (other.isEmpty()) {
            return this;
        }

        List<long[]> all = new ArrayList<>(starts.length + other.starts.length);
        for (int i = 0; i < starts.length; i++) {
            all.add(new long[] { starts[i], ends[i] });
        }
        for (int i = 0; i < other.starts.length; i++) {
            all.add(new long[] { other.starts[i], other.ends[i] });
        }
        return fromUnsorted(all, totalRows);
    }

    /**
     * Complements this RowRanges within [0, totalRows), producing ranges NOT in this set.
     */
    RowRanges complement() {
        if (isEmpty()) {
            return all(totalRows);
        }

        List<long[]> gaps = new ArrayList<>();
        long prev = 0;
        for (int i = 0; i < starts.length; i++) {
            if (prev < starts[i]) {
                gaps.add(new long[] { prev, starts[i] });
            }
            prev = ends[i];
        }
        if (prev < totalRows) {
            gaps.add(new long[] { prev, totalRows });
        }

        long[] rs = new long[gaps.size()];
        long[] re = new long[gaps.size()];
        for (int k = 0; k < gaps.size(); k++) {
            rs[k] = gaps.get(k)[0];
            re[k] = gaps.get(k)[1];
        }
        return new RowRanges(rs, re, totalRows);
    }

    /**
     * Returns the total number of selected rows across all ranges.
     */
    long selectedRowCount() {
        long count = 0;
        for (int i = 0; i < starts.length; i++) {
            count += ends[i] - starts[i];
        }
        return count;
    }

    /**
     * Returns the density: ratio of selected rows to total rows in the row group.
     * A density of 1.0 means all rows are selected (no skipping benefit).
     */
    double density() {
        if (totalRows <= 0) {
            return 1.0;
        }
        return (double) selectedRowCount() / totalRows;
    }

    /**
     * Returns the number of discrete intervals in this selection.
     * High interval counts indicate fragmented selection that may not benefit from skipping.
     */
    int transitionCount() {
        return starts.length;
    }

    boolean isEmpty() {
        return starts.length == 0;
    }

    /**
     * Returns true if this RowRanges covers all rows (equivalent to no skipping).
     */
    boolean isAll() {
        return starts.length == 1 && starts[0] == 0 && ends[0] == totalRows;
    }

    int rangeCount() {
        return starts.length;
    }

    long rangeStart(int index) {
        return starts[index];
    }

    long rangeEnd(int index) {
        return ends[index];
    }

    long totalRows() {
        return totalRows;
    }

    /**
     * Anti-fragmentation check: returns true if the selection is too dense or too fragmented
     * to benefit from page-level skipping. In such cases, reading the full row group is cheaper
     * than the overhead of range-based decode.
     */
    boolean shouldDiscard() {
        return shouldDiscard(DEFAULT_DENSITY_THRESHOLD, DEFAULT_MAX_TRANSITIONS);
    }

    boolean shouldDiscard(double densityThreshold, int maxTransitions) {
        if (isEmpty()) {
            return false;
        }
        return density() > densityThreshold || transitionCount() > maxTransitions;
    }

    /**
     * Returns true if any selected range overlaps the half-open interval {@code [pageStart, pageEnd)}.
     * O(log n) via binary search, allocation-free.
     */
    boolean overlaps(long pageStart, long pageEnd) {
        if (starts.length == 0 || pageStart >= pageEnd) {
            return false;
        }
        int idx = Arrays.binarySearch(starts, pageStart);
        if (idx >= 0) {
            return true;
        }
        int insertionPoint = -idx - 1;
        // Check the range just before the insertion point — its end might extend past pageStart
        if (insertionPoint > 0 && ends[insertionPoint - 1] > pageStart) {
            return true;
        }
        // Check the range at the insertion point — its start might be before pageEnd
        return insertionPoint < starts.length && starts[insertionPoint] < pageEnd;
    }

    /**
     * Returns the first selected row in {@code [rangeStart, rangeEnd)}, or {@code -1} if none.
     */
    long firstSelectedInRange(long rangeStart, long rangeEnd) {
        if (starts.length == 0 || rangeStart >= rangeEnd) {
            return -1;
        }
        int idx = Arrays.binarySearch(starts, rangeStart);
        int i;
        if (idx >= 0) {
            i = idx;
        } else {
            int insertionPoint = -idx - 1;
            // The range before insertionPoint might contain rangeStart
            if (insertionPoint > 0 && ends[insertionPoint - 1] > rangeStart) {
                return rangeStart;
            }
            i = insertionPoint;
        }
        if (i < starts.length && starts[i] < rangeEnd) {
            return Math.max(starts[i], rangeStart);
        }
        return -1;
    }

    /**
     * Returns the last selected row in {@code [rangeStart, rangeEnd)}, or {@code -1} if none.
     */
    long lastSelectedInRange(long rangeStart, long rangeEnd) {
        if (starts.length == 0 || rangeStart >= rangeEnd) {
            return -1;
        }
        // Find the last range that starts before rangeEnd
        int idx = Arrays.binarySearch(starts, rangeEnd);
        int i;
        if (idx >= 0) {
            i = idx - 1;
        } else {
            i = -idx - 2;
        }
        // Walk backwards to find a range that overlaps [rangeStart, rangeEnd)
        while (i >= 0) {
            if (ends[i] > rangeStart && starts[i] < rangeEnd) {
                return Math.min(ends[i], rangeEnd) - 1;
            }
            if (ends[i] <= rangeStart) {
                break;
            }
            i--;
        }
        return -1;
    }

    /**
     * Finds the next contiguous run of selected rows starting from {@code pageOffset},
     * within the next {@code maxRows} rows. Returns a {@link Run} describing how many
     * rows to skip before the run and the run length.
     *
     * <p>If no selected rows remain in the window, returns a run with
     * {@code skipBefore == maxRows} and {@code length == 0}.
     */
    Run nextRunInPage(long pageOffset, int maxRows) {
        if (starts.length == 0 || maxRows <= 0) {
            return new Run(maxRows, 0);
        }
        long windowEnd = pageOffset + maxRows;
        long first = firstSelectedInRange(pageOffset, windowEnd);
        if (first < 0) {
            return new Run(maxRows, 0);
        }
        int skipBefore = (int) (first - pageOffset);

        // Find which range contains 'first'
        int idx = Arrays.binarySearch(starts, first);
        int rangeIdx;
        if (idx >= 0) {
            rangeIdx = idx;
        } else {
            rangeIdx = -idx - 2;
        }
        long runEnd = Math.min(ends[rangeIdx], windowEnd);
        int length = (int) (runEnd - first);
        return new Run(skipBefore, length);
    }

    record Run(int skipBefore, int length) {}

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("RowRanges{");
        for (int i = 0; i < starts.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append('[').append(starts[i]).append(", ").append(ends[i]).append(')');
        }
        sb.append(", total=").append(totalRows).append('}');
        return sb.toString();
    }
}
