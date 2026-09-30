/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSearchResult;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects the fetch results of a search on the coordinating node and charges the {@link CircuitBreaker#REQUEST}
 * breaker for the hits they carry. Every shard's hits stay on the heap until the search response has been built,
 * so the charge is only given back when this collection is released at the end of the search.
 */
final class FetchSearchPhaseResults extends ArraySearchPhaseResults<FetchSearchResult> {

    // Holds the charge off the default path until the fetch paths this does not reach yet are accounted for.
    static final FeatureFlag ACCOUNTING_FEATURE_FLAG = new FeatureFlag("coordinator_fetch_accounting");

    private static final long RELEASED = -1L;

    // Suffixed so a trip here does not read like the data node's own fetch charge in the failure it produces.
    private static final String BREAKER_LABEL = ChildMemoryCircuitBreaker.CATEGORY_FETCH + "[coordinator]";

    private final CircuitBreaker circuitBreaker;
    private final AtomicLong reservedBytes = new AtomicLong();

    FetchSearchPhaseResults(int size, CircuitBreaker circuitBreaker) {
        super(size);
        this.circuitBreaker = circuitBreaker;
    }

    /**
     * Charges the breaker for the hits a shard has just sent back, unless they were assembled on this node and
     * arrived charged. Called from {@link FetchSearchPhase} before the result is handed to the collector, so that
     * a trip leaves the hits to the caller to release.
     *
     * @throws CircuitBreakingException if the coordinating node cannot hold these hits
     */
    void reserve(FetchSearchResult result) {
        // The chunked path handed over what it charged for these hits, and the result gives that back when it is
        // released, so charging here again would hold them twice.
        if (ACCOUNTING_FEATURE_FLAG.isEnabled() == false || result.isChargedOnCoordinator()) {
            return;
        }
        long bytes = 0L;
        for (SearchHit hit : result.hits().getHits()) {
            bytes += hit.ramBytesUsed();
        }
        if (bytes == 0L) {
            return;
        }
        circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
        if (addToReservation(bytes) == false) {
            // A phase failure released this collection while the shard was still in flight, so nothing else will.
            circuitBreaker.addWithoutBreaking(-bytes, BREAKER_LABEL);
        }
    }

    private boolean addToReservation(long bytes) {
        long current;
        do {
            current = reservedBytes.get();
            if (current == RELEASED) {
                return false;
            }
        } while (reservedBytes.compareAndSet(current, current + bytes) == false);
        return true;
    }

    @Override
    protected void doClose() {
        long bytes = reservedBytes.getAndSet(RELEASED);
        if (bytes > 0L) {
            circuitBreaker.addWithoutBreaking(-bytes, BREAKER_LABEL);
        }
    }
}
