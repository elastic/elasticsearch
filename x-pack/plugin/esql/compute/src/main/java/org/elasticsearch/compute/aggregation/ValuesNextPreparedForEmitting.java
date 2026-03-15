/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

public class ValuesNextPreparedForEmitting implements Releasable {
    /**
     * Build for a {@link LongLongHash}.
     */
    static ValuesNextPreparedForEmitting build(BlockFactory blockFactory, IntVector selected, LongLongHash hashes) {
        ValuesNextPreparedForEmitting result = new ValuesNextPreparedForEmitting(blockFactory, selected);
        if (hashes.size() == 0) {
            return result;
        }
        try {
            result.countSelected(hashes);
            int total = result.total();
            result.buildIds(total, hashes);
            ValuesNextPreparedForEmitting done = result;
            result = null;
            return done;
        } finally {
            Releasables.close(result);
        }
    }

    /**
     * Build for a {@link LongHashTable}.
     */
    static ValuesNextPreparedForEmitting build(BlockFactory blockFactory, IntVector selected, LongHashTable hashes) {
        ValuesNextPreparedForEmitting result = new ValuesNextPreparedForEmitting(blockFactory, selected);
        if (hashes.size() == 0) {
            return result;
        }
        try {
            result.countSelected(hashes);
            int total = result.total();
            result.buildIds(total, hashes);
            ValuesNextPreparedForEmitting done = result;
            result = null;
            return done;
        } finally {
            Releasables.close(result);
        }
    }

    private final BlockFactory blockFactory;
    private final IntVector selected;
    private final int min;
    private final int max;
    private int[] selectedCounts;
    int[] ids;
    private long ramBytesUsed = 0;

    private ValuesNextPreparedForEmitting(BlockFactory blockFactory, IntVector selected) {
        this.blockFactory = blockFactory;
        this.selected = selected;
        this.min = selected.min();
        this.max = selected.max();
    }

    public int nextValuesEnd(int group, int nextValuesStart) {
        return selectedCounts != null ? selectedCounts[group - min] : nextValuesStart;
    }

    @Override
    public void close() {
        blockFactory.adjustBreaker(-ramBytesUsed);
    }

    /**
     * Get a count of all selected groups. Count *downwards* so that we can
     * flip the sign on all the actually selected groups. Negative values in
     * this array are always unselected groups.
     */
    private void countSelected(LongLongHash hashes) {
        int selectedCountsLen = max + 1 - min;
        reserveBytesForIntArray(selectedCountsLen);
        this.selectedCounts = new int[selectedCountsLen];
        for (int id = 0; id < hashes.size(); id++) {
            int group = (int) hashes.getKey1(id);
            if (inRange(group)) {
                selectedCounts[group - min]--;
            }
        }
    }

    /**
     * Get a count of all selected groups. Count *downwards* so that we can
     * flip the sign on all the actually selected groups. Negative values in
     * this array are always unselected groups.
     */
    private void countSelected(LongHashTable hashes) {
        int selectedCountsLen = max + 1 - min;
        reserveBytesForIntArray(selectedCountsLen);
        this.selectedCounts = new int[selectedCountsLen];
        for (int id = 0; id < hashes.size(); id++) {
            long both = hashes.get(id);
            int group = (int) (both >>> Float.SIZE);
            if (inRange(group)) {
                selectedCounts[group - min]--;
            }
        }
    }

    /**
     * Total the selected groups and turn the counts into the start index into a sort-of
     * off-by-one running count. It's really the number of values that have been inserted
     * into the results before starting on this group. Unselected groups will still
     * have negative counts.
     * <p>
     *     For example, if
     * </p>
     * {@snippet lang=Markdown :
     * | Group | Value Count | Selected |
     * |-------|-------------|----------|
     * |     0 | 3           | <-       |
     * |     1 | 1           | <-       |
     * |     2 | 2           |          |
     * |     3 | 1           | <-       |
     * |     4 | 4           | <-       |
     * }
     * <p>
     *     Then the total is 9 and the counts array will contain {@code 0, 3, -2, 4, 5}
     * </p>
     */
    private int total() {
        int total = 0;
        for (int s = 0; s < selected.getPositionCount(); s++) {
            int group = selected.getInt(s);
            int count = -selectedCounts[group - min];
            selectedCounts[group - min] = total;
            total += count;
        }
        return total;
    }

    /**
     * Build a list of ids to insert in order *and* convert the running
     * {@code count} in {@code selectedCounts[group - result.min]} into
     * the end index (exclusive) in ids for each group.
     * Here we use the negative counts to signal that a group hasn't been
     * selected and the id containing values for that group is ignored.
     * <p>
     *     For example, if
     * </p>
     * {@snippet lang=Markdown :
     * | Group | Value Count | Selected |
     * |-------|-------------|----------|
     * |     0 | 3           | <-       |
     * |     1 | 1           | <-       |
     * |     2 | 2           |          |
     * |     3 | 1           | <-       |
     * |     4 | 4           | <-       |
     * }
     * <p>
     *     Then the total is 9 and the counts array will start with
     *     {@code 0, 3, -2, 4, 5}. The counts will end with
     *     {@code 3, 4, -2, 5, 9}.
     * </p>
     */
    private void buildIds(int total, LongLongHash hashes) {
        reserveBytesForIntArray(total);
        ids = new int[total];
        for (int id = 0; id < hashes.size(); id++) {
            int group = (int) hashes.getKey1(id);
            if (inRange(group) && selectedCounts[group - min] >= 0) {
                ids[selectedCounts[group - min]++] = id;
            }
        }
    }

    /**
     * Build a list of ids to insert in order *and* convert the running
     * {@code count} in {@code selectedCounts[group - result.min]} into
     * the end index (exclusive) in ids for each group.
     * Here we use the negative counts to signal that a group hasn't been
     * selected and the id containing values for that group is ignored.
     * <p>
     *     For example, if
     * </p>
     * {@snippet lang=Markdown :
     * | Group | Value Count | Selected |
     * |-------|-------------|----------|
     * |     0 | 3           | <-       |
     * |     1 | 1           | <-       |
     * |     2 | 2           |          |
     * |     3 | 1           | <-       |
     * |     4 | 4           | <-       |
     * }
     * <p>
     *     Then the total is 9 and the counts array will start with
     *     {@code 0, 3, -2, 4, 5}. The counts will end with
     *     {@code 3, 4, -2, 5, 9}.
     * </p>
     */
    private void buildIds(int total, LongHashTable hashes) {
        reserveBytesForIntArray(total);
        ids = new int[total];
        for (int id = 0; id < hashes.size(); id++) {
            long both = hashes.get(id);
            int group = (int) (both >>> Float.SIZE);
            if (inRange(group) && selectedCounts[group - min] >= 0) {
                ids[selectedCounts[group - min]++] = id;
            }
        }
    }

    private void reserveBytesForIntArray(long numElements) {
        long adjust = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + numElements * Integer.BYTES);
        blockFactory.adjustBreaker(adjust);
        ramBytesUsed += adjust;
    }

    private boolean inRange(int group) {
        return min <= group && group <= max;
    }
}
