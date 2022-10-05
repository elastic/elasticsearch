/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.metrics;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

public class DoubleMeanMetric {
    private final DoubleAdder sum = new DoubleAdder();
    private final LongAdder counter = new LongAdder();

    public void inc(double value) {
        sum.add(value);
        counter.increment();
    }

    private long count() {
        final long count = counter.sum();
        assert count >= 0 : "Count of MeanMetric must always be non-negative; got " + count;
        return count;
    }

    public DoubleMean mean() {
        long count = count();
        if (count > 0) {
            return new DoubleMean(sum.sum(), count);
        }
        return DoubleMean.ZERO;
    }
}
