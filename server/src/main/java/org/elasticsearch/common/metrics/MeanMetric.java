/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.metrics;

import java.util.concurrent.atomic.LongAdder;

public final class MeanMetric {

    private final LongAdder counter = new LongAdder();
    private final LongAdder sum = new LongAdder();

    public void inc(long n) {
        counter.increment();
        sum.add(n);
    }

    /**
     * Returns the current count of this metric. This metric supports only {@link #inc(long)} that increases the counter
     * whenever it's invoked; hence, the returned count is always non-negative.
     */
    public long count() {
        final long count = counter.sum();
        assert count >= 0 : "Count of MeanMetric must always be non-negative; got " + count;
        return count;
    }

    public long sum() {
        return sum.sum();
    }

    public double mean() {
        long count = count();
        if (count > 0) {
            return sum.sum() / (double) count;
        }
        return 0.0;
    }
}
