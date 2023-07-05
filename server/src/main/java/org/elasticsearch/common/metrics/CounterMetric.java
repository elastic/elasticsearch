/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.metrics;

import org.elasticsearch.core.Assertions;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * A {@link CounterMetric} is used to track the number of completed and outstanding items, for example, the number of executed refreshes,
 * the currently used memory by indexing, the current pending search requests. In both cases, the current {@link CounterMetric#count} is
 * always non-negative.
 */
public final class CounterMetric {
    private final LongAdder counter = new LongAdder();
    private final AtomicLong assertingCounter = Assertions.ENABLED ? new AtomicLong() : null;

    private static boolean assertNonNegative(long n) {
        assert n >= 0 : "CounterMetric value must always be non-negative; got: " + n;
        return true;
    }

    public void inc() {
        counter.increment();
        assert assertNonNegative(assertingCounter.incrementAndGet());
    }

    public void inc(long n) {
        counter.add(n);
        assert assertNonNegative(assertingCounter.addAndGet(n));
    }

    public void dec() {
        counter.decrement();
        assert assertNonNegative(assertingCounter.decrementAndGet());
    }

    public void dec(long n) {
        counter.add(-n);
        assert assertNonNegative(assertingCounter.addAndGet(-n));
    }

    /**
     * Returns the current count of this metric. The returned value is always non-negative.
     * <p>
     * As this metric is implemented using a {@link LongAdder}, the returned value is NOT an atomic snapshot;
     * invocation in the absence of concurrent updates returns an accurate result, but concurrent updates that
     * occur while the sum is being calculated might not be incorporated.
     *
     * @see LongAdder#sum()
     */
    public long count() {
        // The `counter.sum()` value is expected to always be non-negative. And if it's negative, then some concurrent updates
        // aren't incorporated yet. In this case, we can immediately return 0L; but here we choose to retry several times
        // to hopefully have a more accurate value than 0L.
        for (int i = 0; i < 5; i++) {
            final long count = counter.sum();
            if (count >= 0L) {
                return count;
            }
        }
        return 0L;
    }
}
