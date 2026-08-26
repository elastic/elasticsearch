/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BinarySearcher;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Aggregates the top N collected {@link BytesRef} values, kept sorted.
 * <p>
 *     Collection is O(1) for values out of the current top N. For values better than the worst value, it's O(log(n))
 *     for the binary search plus O(n) for the shift on insertion (n is bounded by {@code limit}).
 * </p>
 * <p>
 *     Backing storage is an array of {@link BreakingBytesRefBuilder}, one per slot, sized at most {@code limit}.
 *     Each slot owns its bytes; on insertion, references are shifted right and the displaced (worst) slot is
 *     reused as the destination for the new value.
 * </p>
 */
public class BytesRefTopNSet implements Releasable {

    private static final String BREAKER_LABEL = "BytesRefTopNSet";

    private final CircuitBreaker breaker;
    private final SortOrder order;
    private int limit;

    /**
     * One builder per slot. Created lazily as values are collected, up to {@code limit} entries.
     * Index {@code 0} is the best value, index {@code count - 1} is the worst.
     */
    private final BreakingBytesRefBuilder[] values;
    private final BytesRefBinarySearcher searcher;

    private int count;

    public BytesRefTopNSet(BigArrays bigArrays, SortOrder order, int limit) {
        this.breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        this.order = order;
        this.limit = limit;
        this.count = 0;
        this.values = new BreakingBytesRefBuilder[limit];
        this.searcher = new BytesRefBinarySearcher();
    }

    /**
     * Adds the value to the top N, as long as it is "better" than the worst value, or the top isn't full yet.
     */
    public boolean collect(BytesRef value) {
        if (limit == 0) {
            return false;
        }

        // Short-circuit if the value is worse than the worst value on the top.
        // This avoids a O(log(n)) check in the binary search.
        if (count == limit && betterThan(getWorstValueView(), value)) {
            return false;
        }

        if (count == 0) {
            BreakingBytesRefBuilder slot = newSlotWithBytes(value);
            values[0] = slot;
            count++;
            return true;
        }

        int insertionIndex = searcher.search(0, count - 1, value);

        if (insertionIndex == count - 1) {
            if (betterThan(getWorstValueView(), value)) {
                BreakingBytesRefBuilder slot = newSlotWithBytes(value);
                values[count] = slot;
                count++;
                return true;
            }
        }

        // Only unique values are stored here.
        if (values[insertionIndex].bytesRefView().bytesEquals(value)) {
            return true;
        }

        // Allocate the new slot (with its bytes already populated) BEFORE mutating values[]. If allocation or copying
        // throws (e.g. circuit breaker exception), the array stays in a valid state.
        BreakingBytesRefBuilder newEntry = newSlotWithBytes(value);

        // The searcher returns the upper bound, so we move right the elements from there.
        // When the array is full, the displaced worst slot is closed; otherwise it grows by one.
        BreakingBytesRefBuilder displaced = (count == limit) ? values[count - 1] : null;
        int lastIndex = Math.min(count, limit - 1);
        for (int i = lastIndex; i > insertionIndex; i--) {
            values[i] = values[i - 1];
        }
        values[insertionIndex] = newEntry;
        count = Math.min(count + 1, limit);

        if (displaced != null) {
            displaced.close();
        }

        return true;
    }

    /**
     * Allocate a fresh slot already populated with {@code value}. If allocation or copying throws, any partially
     * acquired breaker memory is released before propagating the exception.
     */
    private BreakingBytesRefBuilder newSlotWithBytes(BytesRef value) {
        BreakingBytesRefBuilder slot = new BreakingBytesRefBuilder(breaker, BREAKER_LABEL, value.length);
        boolean success = false;
        try {
            slot.copyBytes(value);
            success = true;
            return slot;
        } finally {
            if (success == false) {
                slot.close();
            }
        }
    }

    /**
     * Reduces the limit of the top N by 1.
     * <p>
     *     This method is specifically used to count for the null value, and ignore the extra element here without extra cost.
     * </p>
     */
    public void reduceLimitByOne() {
        if (limit == 0) {
            return;
        }
        limit--;
        if (count > limit) {
            // Free the displaced slot: it won't fit anymore.
            assert count == limit + 1 : "expected exactly one extra slot, got count=" + count + " limit=" + limit;
            Releasables.close(values[count - 1]);
            values[count - 1] = null;
            count = limit;
        }
    }

    /**
     * Returns a deep copy of the worst value in the top.
     * <p>
     *     The worst is the greatest value for {@link SortOrder#ASC}, and the lowest value for {@link SortOrder#DESC}.
     * </p>
     * <p>
     *     This always allocates. Hot callers that only need to compare against the worst value should prefer
     *     {@link #getWorstValueView()} — which returns a view that is invalidated by the next mutation of this set.
     * </p>
     */
    public BytesRef getWorstValue() {
        assert count > 0;
        return BytesRef.deepCopyOf(values[count - 1].bytesRefView());
    }

    /**
     * Returns a non-owning {@link BytesRef} view of the worst value in the top.
     * <p>
     *     The returned reference is only valid until the next mutating call on this set
     *     ({@link #collect(BytesRef)}, {@link #reduceLimitByOne()}, {@link #close()}). Callers that need to
     *     keep the bytes across mutations must use {@link #getWorstValue()} instead.
     * </p>
     */
    public BytesRef getWorstValueView() {
        assert count > 0;
        return values[count - 1].bytesRefView();
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

    /**
     * {@code true} if {@code lhs} is "better" than {@code rhs}.
     * "Better" in this means "lower" for {@link SortOrder#ASC} and "higher" for {@link SortOrder#DESC}.
     */
    private boolean betterThan(BytesRef lhs, BytesRef rhs) {
        return order.reverseMul() * lhs.compareTo(rhs) < 0;
    }

    @Override
    public void close() {
        Releasables.close(values);
    }

    private final class BytesRefBinarySearcher extends BinarySearcher {

        private BytesRef searchFor;

        @Override
        protected int compare(int index) {
            assert searchFor != null;
            return order.reverseMul() * values[index].bytesRefView().compareTo(searchFor);
        }

        @Override
        protected int getClosestIndex(int index1, int index2) {
            return Math.max(index1, index2);
        }

        @Override
        protected double distance(int index) {
            throw new UnsupportedOperationException("getClosestIndex() is overridden and doesn't depend on this");
        }

        int search(int from, int to, BytesRef value) {
            this.searchFor = value;
            try {
                return super.search(from, to);
            } finally {
                this.searchFor = null;
            }
        }
    }
}
