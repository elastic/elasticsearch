/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BinarySearcher;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Aggregates the top N collected values, and keeps them sorted.
 * <p>
 *     Collection is O(1) for values out of the current top N. For values better than the worst value, it's O(log(n)).
 * </p>
 */
public class LongTopNSet implements Releasable {

    private final SortOrder order;
    private int limit;

    private final LongArray values;
    private final LongBinarySearcher searcher;
    private final LongHash seenValues;

    private int count;

    public LongTopNSet(BigArrays bigArrays, SortOrder order, int limit) {
        this.order = order;
        this.limit = limit;
        this.count = 0;
        boolean success = false;
        try {
            this.values = bigArrays.newLongArray(limit, false);
            this.searcher = new LongBinarySearcher(values, order);
            this.seenValues = new LongHash(limit, 0.05f, bigArrays);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    /**
     * Adds the value to the top N, as long as it is "better" than the worst value, or the top isn't full yet.
     */
    public boolean collect(long value) {
        if (limit == 0) {
            return false;
        }

        // Short-circuit if the value is worse than the worst value on the top.
        // This avoids a O(log(n)) check in the binary search
        if (count == limit && betterThan(getWorstValue(), value)) {
            return false;
        }

        if (count == 0) {
            values.set(0, value);
            count++;
            return true;
        }

        if (seenValues.add(value) < 0) {
            // The value was already added
            return true;
        }

        int insertionIndex = this.searcher.search(0, count - 1, value);

        if (insertionIndex == count - 1) {
            if (betterThan(getWorstValue(), value)) {
                values.set(count, value);
                count++;
                return true;
            }
        }

        if (values.get(insertionIndex) == value) {
            // Only unique values are stored here
            return true;
        }

        // The searcher returns the upper bound, so we move right the elements from there
        for (int i = Math.min(count, limit - 1); i > insertionIndex; i--) {
            values.set(i, values.get(i - 1));
        }

        values.set(insertionIndex, value);
        count = Math.min(count + 1, limit);

        return true;
    }

    /**
     * Reduces the limit of the top N by 1.
     * <p>
     *     This method is specifically used to count for the null value, and ignore the extra element here without extra cost.
     * </p>
     */
    public void reduceLimitByOne() {
        limit--;
        count = Math.min(count, limit);
    }

    /**
     * Returns the worst value in the top.
     * <p>
     *     The worst is the greatest value for {@link SortOrder#ASC}, and the lowest value for {@link SortOrder#DESC}.
     * </p>
     */
    public long getWorstValue() {
        assert count > 0;
        return values.get(count - 1);
    }

    /**
     * The order of the sort.
     */
    public SortOrder getOrder() {
        return order;
    }

    public int getLimit() {
        return limit;
    }

    public int getCount() {
        return count;
    }

    private static class LongBinarySearcher extends BinarySearcher {

        final LongArray array;
        final SortOrder order;
        long searchFor;

        LongBinarySearcher(LongArray array, SortOrder order) {
            this.array = array;
            this.order = order;
            this.searchFor = Integer.MIN_VALUE;
        }

        @Override
        protected int compare(int index) {
            // Prevent use of BinarySearcher.search() and force the use of DoubleBinarySearcher.search()
            assert this.searchFor != Integer.MIN_VALUE;

            return order.reverseMul() * Long.compare(array.get(index), searchFor);
        }

        @Override
        protected int getClosestIndex(int index1, int index2) {
            // Overridden to always return the upper bound
            return Math.max(index1, index2);
        }

        @Override
        protected double distance(int index) {
            throw new UnsupportedOperationException("getClosestIndex() is overridden and doesn't depend on this");
        }

        public int search(int from, int to, long searchFor) {
            this.searchFor = searchFor;
            return super.search(from, to);
        }
    }

    /**
     * {@code true} if {@code lhs} is "better" than {@code rhs}.
     * "Better" in this means "lower" for {@link SortOrder#ASC} and "higher" for {@link SortOrder#DESC}.
     */
    private boolean betterThan(long lhs, long rhs) {
        return getOrder().reverseMul() * Long.compare(lhs, rhs) < 0;
    }

    @Override
    public final void close() {
        Releasables.close(values, seenValues);
    }
}
