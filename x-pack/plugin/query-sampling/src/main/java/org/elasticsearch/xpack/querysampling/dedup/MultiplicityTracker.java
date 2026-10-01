/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counts how often each distinct query was captured, so a repeated query is stored once together with its
 * multiplicity instead of once per arrival. The multiplicity is what keeps estimates about the traffic
 * unbiased after duplicates are thrown away.
 * <p>
 * The number of distinct queries held is bounded. Once the bound is reached, queries that are not yet
 * known are not tracked and are only counted as such; queries already known keep being counted.
 */
public final class MultiplicityTracker {

    private final int maxDistinct;
    private final Map<QueryFingerprint, Long> multiplicities = new ConcurrentHashMap<>();
    private final LongAdder untracked = new LongAdder();

    public MultiplicityTracker(int maxDistinct) {
        this.maxDistinct = maxDistinct;
    }

    /**
     * Records one more arrival of the query.
     *
     * @return how many times the query has been seen including this arrival, or 0 if the tracker is full
     *         and the query was not known before
     */
    public long record(QueryFingerprint fingerprint) {
        // only the single consumer thread records, so the size check cannot race with another insert
        if (multiplicities.size() >= maxDistinct && multiplicities.containsKey(fingerprint) == false) {
            untracked.increment();
            return 0;
        }
        return multiplicities.merge(fingerprint, 1L, Long::sum);
    }

    public int distinct() {
        return multiplicities.size();
    }

    public long untracked() {
        return untracked.sum();
    }
}
