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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSearchResult;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects the fetch results of a search on the coordinating node and owns the {@link CircuitBreaker#REQUEST}
 * charge for the hits they carry. The charge moves to the {@link SearchResponse} once one is built;
 * {@link #doClose()} only releases it if one never is.
 */
final class FetchSearchPhaseResults extends ArraySearchPhaseResults<FetchSearchResult> {

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
     * Takes ownership of the charge for the hits a shard has just sent back, estimating them here unless they
     * arrived already charged. Called before the result reaches the collector, so that a trip leaves the hits
     * to the caller to release.
     *
     * @throws CircuitBreakingException if the coordinating node cannot hold these hits
     */
    void reserve(FetchSearchResult result) {
        final long bytes;
        if (result.isChargedOnCoordinator()) {
            // The chunked path already charged these hits, so take that charge over instead of estimating again.
            bytes = result.transferCoordinatorCharge(circuitBreaker);
        } else {
            long estimated = 0L;
            for (SearchHit hit : result.hits().getHits()) {
                estimated += hit.ramBytesUsed();
            }
            if (estimated == 0L) {
                return;
            }
            circuitBreaker.addEstimateBytesAndMaybeBreak(estimated, BREAKER_LABEL);
            bytes = estimated;
        }
        if (bytes > 0L && addToReservation(bytes) == false) {
            // A phase failure released this collection while the shard was still in flight, so nothing else will.
            circuitBreaker.addWithoutBreaking(-bytes, BREAKER_LABEL);
        }
    }

    /**
     * Hands the outstanding charge to the caller; a late {@link #reserve} or {@link #doClose()} will not touch it
     * again. Must not be called until the caller is committed to using the result, since discarding it then leaks.
     *
     * @return the outstanding charge, or {@code null} if there is none
     */
    @Nullable
    Releasable transferCharge() {
        long bytes = reservedBytes.getAndSet(RELEASED);
        if (bytes <= 0L) {
            return null;
        }
        return Releasables.assertOnce(() -> circuitBreaker.addWithoutBreaking(-bytes, BREAKER_LABEL));
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
