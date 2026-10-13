/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Numbers the origins of the documents of one query, so an operator that keeps rows across pages, like a TopN, can store
 * the origin of a row as an int and give the row its origin back later. The pages come from many nodes, and each has
 * its own dictionary of origins, so the numbers of one page mean nothing for the next.
 * <p>
 * The table only grows, by the origins rows bring, and lives until the query ends. Its memory is accounted on the breaker
 * it gets, and {@link #close} returns it.
 */
public final class DocOriginTable implements Releasable {
    /**
     * The map entry and the list slot of an origin, on top of the origin itself.
     */
    private static final long ENTRY_OVERHEAD = 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + Integer.BYTES + RamUsageEstimator.shallowSizeOfInstance(Integer.class);

    private final CircuitBreaker breaker;
    private final Map<DocOrigin, Integer> ordinals = new HashMap<>();
    private final List<DocOrigin> origins = new ArrayList<>();
    private long reserved;
    private boolean closed;

    public DocOriginTable(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    /**
     * The number of {@code origin}, numbered now if it is new.
     *
     * @throws IllegalStateException if the origin is new and the table is closed
     */
    public synchronized int ordinal(DocOrigin origin) {
        Integer existing = ordinals.get(origin);
        if (existing != null) {
            return existing;
        }
        if (closed) {
            // a driver that outlives the query must not reserve bytes nobody returns
            throw new IllegalStateException("the document origin table is closed");
        }
        long bytes = origin.ramBytesUsed() + ENTRY_OVERHEAD;
        breaker.addEstimateBytesAndMaybeBreak(bytes, "esql document origins");
        reserved += bytes;
        int ordinal = origins.size();
        origins.add(origin);
        ordinals.put(origin, ordinal);
        return ordinal;
    }

    /**
     * The origin numbered {@code ordinal}.
     */
    public synchronized DocOrigin get(int ordinal) {
        return origins.get(ordinal);
    }

    /**
     * The bytes the table holds on its breaker.
     */
    public synchronized long ramBytesUsed() {
        return reserved;
    }

    @Override
    public synchronized void close() {
        closed = true;
        breaker.addWithoutBreaking(-reserved);
        reserved = 0;
    }
}
