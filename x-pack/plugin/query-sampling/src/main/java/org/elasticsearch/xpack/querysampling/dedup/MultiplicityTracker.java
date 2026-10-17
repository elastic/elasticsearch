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
 * estimates unbiased. Queries are forgotten between one and two windows after their last arrival (two generations,
 * rotated every window). The number of distinct queries is bounded; unknown queries beyond it are only counted as untracked.
 */
public final class MultiplicityTracker {

    private final int maxDistinct;
    private volatile long windowNanos;
    private final LongSupplier nanoTime;
    private volatile Map<QueryFingerprint, TrackedQuery> current = new ConcurrentHashMap<>();
    private volatile Map<QueryFingerprint, TrackedQuery> previous = new ConcurrentHashMap<>();
    private long windowStart;
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
        TrackedQuery query = current.get(fingerprint);
        if (query == null) {
            query = previous.remove(fingerprint);
            if (query == null && distinct() >= maxDistinct) {
                untracked.increment();
                return null;
            }
            if (query == null) {
                query = new TrackedQuery();
            }
            current.put(fingerprint, query);
        }
        query.recordArrival(1.0 / captureRate);
        return query;
    }

    private void rotateIfDue() {
        long now = nanoTime.getAsLong();
        long elapsed = now - windowStart;
        long window = windowNanos;
        if (elapsed >= window) {
            // after a quiet spell of two windows or more the current generation is out of date too
            previous = elapsed / window >= 2 ? new ConcurrentHashMap<>() : current;
            current = new ConcurrentHashMap<>();
            windowStart = now;
        }
    }

    // once false, the query's counters no longer change: its next arrival starts a new count
    public boolean isTracking(QueryFingerprint fingerprint, TrackedQuery query) {
        return current.get(fingerprint) == query || previous.get(fingerprint) == query;
    }

    public int distinct() {
        return current.size() + previous.size();
    }

    public long untracked() {
        return untracked.sum();
    }
}
