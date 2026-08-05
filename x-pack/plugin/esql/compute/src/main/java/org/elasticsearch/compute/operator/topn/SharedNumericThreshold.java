/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.operator.SideChannel;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe, shared numeric TopN threshold for operators running on the same node.
 * <p>
 * Each {@link NumericTopNOperator} publishes the raw value at the top of its full heap. The
 * accumulated value is monotonic: ascending sorts keep the smallest observed heap top and
 * descending sorts keep the largest observed heap top. Readers can use that threshold to skip
 * row groups or stripes whose min/max statistics prove they cannot contain a globally
 * competitive row.
 */
public final class SharedNumericThreshold extends SideChannel {
    public static final class Supplier extends SideChannel.Supplier<SharedNumericThreshold> {
        private final boolean ascending;
        private final boolean nullsFirst;

        public Supplier(boolean ascending, boolean nullsFirst) {
            this.ascending = ascending;
            this.nullsFirst = nullsFirst;
        }

        public boolean ascending() {
            return ascending;
        }

        public boolean nullsFirst() {
            return nullsFirst;
        }

        @Override
        protected SharedNumericThreshold build() {
            return new SharedNumericThreshold(ascending, this);
        }
    }

    private final boolean ascending;
    private final LongAccumulator threshold;
    private final LongAdder offered = new LongAdder();
    private volatile boolean noFurtherCandidates;

    private SharedNumericThreshold(boolean ascending, Supplier supplier) {
        super(supplier);
        this.ascending = ascending;
        this.threshold = ascending ? new LongAccumulator(Math::min, Long.MAX_VALUE) : new LongAccumulator(Math::max, Long.MIN_VALUE);
    }

    /**
     * Publish a raw heap-top value. The accumulator only tightens, never relaxes.
     */
    public void offer(long rawTopOfHeap) {
        threshold.accumulate(rawTopOfHeap);
        offered.increment();
    }

    public long current() {
        return threshold.get();
    }

    /**
     * Whether a min/max statistics range is strictly dominated by the current threshold.
     * Equality is deliberately not skipped because row-position tiebreakers can still matter.
     */
    public boolean dominates(long rangeMin, long rangeMax) {
        long current = current();
        if (ascending) {
            return current != Long.MAX_VALUE && rangeMin > current;
        }
        return current != Long.MIN_VALUE && rangeMax < current;
    }

    /**
     * Mark the source as exhausted by K nulls under NULLS FIRST and collapse the threshold to
     * a value that dominates every ordinary min/max range.
     */
    public void markNoFurtherCandidates() {
        noFurtherCandidates = true;
        threshold.accumulate(ascending ? Long.MIN_VALUE : Long.MAX_VALUE);
    }

    public boolean noFurtherCandidates() {
        return noFurtherCandidates;
    }

    public long offeredCount() {
        return offered.sum();
    }

    @Override
    protected void closeSideChannel() {}
}
