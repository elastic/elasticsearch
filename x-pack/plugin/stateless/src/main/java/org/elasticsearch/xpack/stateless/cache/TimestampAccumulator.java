/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

/**
 * Accumulates counts keyed by timestamp (e.g., epoch millis).
 * <p>
 * Can be called to sum counts on a given time window. Can be used at node level for cache boost level
 * quota tracking (expected totals from shard data, and actual totals from cache residency).
 * Callers pass arbitrary deltas via {@link #accumulate}.
 * Implementations are agnostic to what is being counted (the expectation is that it is cache regions).
 */
public interface TimestampAccumulator {

    /**
     * Adds {@code delta} (can be negative) to the sum associated with {@code timestampMillis}.
     */
    void accumulate(long timestampMillis, long delta);

    /**
     * Returns the sum of counts in the interval {@code [startMillis, endMillis)}.
     * The query range may be clamped to the implementation's retained history before summing.
     */
    long sum(long startMillis, long endMillis);
}
