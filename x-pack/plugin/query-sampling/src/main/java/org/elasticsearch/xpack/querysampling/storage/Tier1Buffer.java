/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Short-term, in-memory home of the queries picked for the sample, until their ground truth is computed
 * and they can be kept for good.
 * <p>
 * The buffer is bounded. When it is full, newly picked queries are turned away and counted. A query that
 * was turned away has already been marked as picked, so it does not get another chance and its inclusion
 * probability is overstated; the acceptance regulation planned for the sampler is meant to keep the buffer
 * from filling up, which keeps that error small.
 */
public final class Tier1Buffer {

    private final int capacity;
    private final Map<QueryFingerprint, SampledQuery> queries = new ConcurrentHashMap<>();
    private final LongAdder rejected = new LongAdder();

    public Tier1Buffer(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Adds a picked query. Must only be called from one thread at a time.
     *
     * @return whether it was stored, which is not the case if the buffer is full
     */
    public boolean add(SampledQuery query) {
        if (queries.size() >= capacity) {
            rejected.increment();
            return false;
        }
        queries.put(query.fingerprint(), query);
        return true;
    }

    public int size() {
        return queries.size();
    }

    public long rejected() {
        return rejected.sum();
    }
}
