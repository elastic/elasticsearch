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

    static RowRanges all(long rowCount) {
        if (rowCount <= 0) {
            return new RowRanges(new long[0], new long[0], 0);
        }
        return new RowRanges(new long[] { 0 }, new long[] { rowCount }, rowCount);
    }

    static RowRanges of(long start, long end, long totalRows) {
        if (start >= end) {
            return new RowRanges(new long[0], new long[0], totalRows);
        }
        return new RowRanges(new long[] { start }, new long[] { end }, totalRows);
    }

    static RowRanges ofSorted(long[] starts, long[] ends, long totalRows) {
        return new RowRanges(starts.clone(), ends.clone(), totalRows);
    }

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

        long[] s = new long[merged.size()];
        long[] e = new long[merged.size()];
        for (int i = 0; i < merged.size(); i++) {
            s[i] = merged.get(i)[0];
            e[i] = merged.get(i)[1];
        }
        return new RowRanges(s, e, totalRows);
    }

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

    long selectedRowCount() {
        long count = 0;
        for (int i = 0; i < starts.length; i++) {
            count += ends[i] - starts[i];
        }
        return count;
    }

    double density() {
        if (totalRows <= 0) {
            return 1.0;
        }
        return (double) selectedRowCount() / totalRows;
    }

    int transitionCount() {
        return starts.length;
    }

    boolean isEmpty() {
        return starts.length == 0;
    }

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
        if (insertionPoint > 0 && ends[insertionPoint - 1] > pageStart) {
            return true;
        }
        return insertionPoint < starts.length && starts[insertionPoint] < pageEnd;
    }

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

    long lastSelectedInRange(long rangeStart, long rangeEnd) {
        if (starts.length == 0 || rangeStart >= rangeEnd) {
            return -1;
        }
        int idx = Arrays.binarySearch(starts, rangeEnd);
        int i;
        if (idx >= 0) {
            i = idx - 1;
        } else {
            i = -idx - 2;
        }
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
