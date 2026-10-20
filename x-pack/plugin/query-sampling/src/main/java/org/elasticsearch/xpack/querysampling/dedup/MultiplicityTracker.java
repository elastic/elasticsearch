/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Counts arrivals per distinct query so duplicates can be stored once with their multiplicity, which keeps traffic
 * estimates unbiased. Queries are forgotten between one and two windows after their last arrival: each remembers the
 * window of its last arrival, and at the end of a window the ones from two windows ago are removed. The number of
 * distinct queries is bounded; unknown queries beyond it are only counted as untracked.
 */
public final class MultiplicityTracker {

    private final int maxDistinct;
    private volatile long windowNanos;
    private final LongSupplier nanoTime;
    private final Map<QueryFingerprint, TrackedQuery> tracked = new ConcurrentHashMap<>();
    // both only used by the thread that records
    private long windowStart;
    private long window;
    private final LongAdder untracked = new LongAdder();

    public MultiplicityTracker(int maxDistinct) {
        this(maxDistinct, TimeValue.MAX_VALUE, System::nanoTime);
    }

    public MultiplicityTracker(int maxDistinct, TimeValue window, LongSupplier nanoTime) {
        this.maxDistinct = maxDistinct;
        this.windowNanos = window.nanos();
        this.nanoTime = nanoTime;
        this.windowStart = nanoTime.getAsLong();
    }

    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.MULTIPLICITY_WINDOW, value -> this.windowNanos = value.nanos());
    }

    @Nullable
    public TrackedQuery record(QueryFingerprint fingerprint) {
        return record(fingerprint, 1.0);
    }

    /**
     * Must only be called from one thread at a time.
     *
     * @return the query including this arrival, or {@code null} if the tracker is full and the query was not known
     */
    @Nullable
    public TrackedQuery record(QueryFingerprint fingerprint, double captureRate) {
        rotateIfDue();
        TrackedQuery query = tracked.get(fingerprint);
        if (query == null) {
            if (distinct() >= maxDistinct) {
                untracked.increment();
                return null;
            }
            query = new TrackedQuery();
            tracked.put(fingerprint, query);
        }
        query.lastWindow = window;
        query.recordArrival(1.0 / captureRate);
        return query;
    }

    private void rotateIfDue() {
        long now = nanoTime.getAsLong();
        long elapsed = now - windowStart;
        long windowLength = windowNanos;
        if (elapsed >= windowLength) {
            // after a quiet spell of two windows or more every query is out of date
            window += Math.min(2, elapsed / windowLength);
            tracked.values().removeIf(query -> query.lastWindow < window - 1);
            windowStart = now;
        }
    }

    // once false, the query's counters no longer change: its next arrival starts a new count
    public boolean isTracking(QueryFingerprint fingerprint, TrackedQuery query) {
        return tracked.get(fingerprint) == query;
    }

    public int distinct() {
        return tracked.size();
    }

    public long untracked() {
        return untracked.sum();
    }
}
