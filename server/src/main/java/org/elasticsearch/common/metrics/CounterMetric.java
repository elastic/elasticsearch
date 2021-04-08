/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.metrics;

import org.elasticsearch.Assertions;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class CounterMetric {
    private final LongAdder counter = new LongAdder();
    private final AtomicLong assertingCounter = Assertions.ENABLED ? new AtomicLong() : null;

    private boolean assertNonNegative(long n) {
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

    public long count() {
        // Retries up to 5 times as the returned value of LongAdder#sum is NOT an atomic snapshot.
        for (int i = 0; i < 5; i++) {
            final long count = counter.sum();
            if (count >= 0L) {
                return count;
            }
        }
        return 0L;
    }
}
