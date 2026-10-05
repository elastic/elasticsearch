/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Counts how often each distinct query was captured, so a repeated query is stored once together with its
 * multiplicity instead of once per arrival. The multiplicity is what keeps estimates about the traffic
 * unbiased after duplicates are thrown away.
 * <p>
 * Queries are forgotten once they have not been seen for a while, so the counts follow the current traffic
 * and the tracker does not fill up with queries that are gone. This is done in two generations: queries
 * live in the current one, and every {@code window} the current one becomes the previous one and the old
 * previous one is dropped. A query seen again is moved into the current generation, so it keeps its count
 * for as long as it keeps arriving, and is forgotten between one and two windows after its last arrival.
 * Queries that were picked for the sample keep their counters, they are referenced from the sample.
 * <p>
 * The number of distinct queries held is bounded. Once the bound is reached, queries that are not yet
 * known are not tracked and are only counted as such, until a rotation makes room; queries already known
 * keep being counted.
 */
public final class MultiplicityTracker {

    private final int maxDistinct;
    private final long windowNanos;
    private final LongSupplier nanoTime;
    private volatile Map<QueryFingerprint, TrackedQuery> current = new ConcurrentHashMap<>();
    private volatile Map<QueryFingerprint, TrackedQuery> previous = new ConcurrentHashMap<>();
    private long windowStart;
    private final LongAdder untracked = new LongAdder();

    /**
     * A tracker that never forgets a query.
     */
    public MultiplicityTracker(int maxDistinct) {
        this(maxDistinct, TimeValue.MAX_VALUE, System::nanoTime);
    }

    public MultiplicityTracker(int maxDistinct, TimeValue window, LongSupplier nanoTime) {
        this.maxDistinct = maxDistinct;
        this.windowNanos = window.nanos();
        this.nanoTime = nanoTime;
        this.windowStart = nanoTime.getAsLong();
    }

    /**
     * Records one more arrival of a query that was certain to be captured. Must only be called from one
     * thread at a time.
     */
    @Nullable
    public TrackedQuery record(QueryFingerprint fingerprint) {
        return record(fingerprint, 1.0);
    }

    /**
     * Records one more arrival of the query. Must only be called from one thread at a time.
     *
     * @param captureRate the probability the arrival had of being captured
     * @return the query with its multiplicity including this arrival, or {@code null} if the tracker is
     *         full and the query was not known before
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
        if (elapsed >= windowNanos) {
            // after a quiet spell of two windows or more the current generation is already out of date too
            previous = elapsed / windowNanos >= 2 ? new ConcurrentHashMap<>() : current;
            current = new ConcurrentHashMap<>();
            windowStart = now;
        }
    }

    public int distinct() {
        return current.size() + previous.size();
    }

    public long untracked() {
        return untracked.sum();
    }
}
