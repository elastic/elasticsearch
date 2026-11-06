/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import org.elasticsearch.core.Nullable;

/**
 * Counts events, bytes or document ids for one {@link CounterMetric} in one collector. It has one writer at a time, so
 * the value is a plain field. The counter of a detail also adds every value to the counter of its parent.
 */
public final class Counter {
    private long value;
    @Nullable
    private final Counter parent;

    Counter(@Nullable Counter parent) {
        this.parent = parent;
    }

    /** Adds {@code delta}, which must not be negative. The count saturates at {@code Long.MAX_VALUE}. */
    public void add(long delta) {
        assert delta >= 0 : "a count only grows, got [" + delta + "]";
        final long sum = value + delta;
        value = sum < 0 ? Long.MAX_VALUE : sum;
        if (parent != null) {
            parent.add(delta);
        }
    }

    long value() {
        return value;
    }
}
