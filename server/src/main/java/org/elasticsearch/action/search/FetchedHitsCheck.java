/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.search.SearchHit;

import java.util.List;

/**
 * Checks the hits the coordinating node collects in the fetch phase of a search, as they arrive. A caller that can't use a response
 * past some size, such as async search when the response is stored, can fail the search here instead of after the response is built.
 * <p>
 * Called concurrently from transport threads, so implementations must be thread-safe and must not make other shards' calls wait on
 * slow work.
 */
@FunctionalInterface
public interface FetchedHitsCheck {

    FetchedHitsCheck NOOP = (hits, bytes) -> {};

    /**
     * Called with hits the coordinating node has just charged to the circuit breaker. Hits that arrive in chunks are checked per chunk.
     *
     * @param hits  the hits that just arrived
     * @param bytes the estimated heap size of these hits, the amount charged for them
     * @throws RuntimeException to fail the search the way a circuit breaker trip does
     */
    void onHits(List<SearchHit> hits, long bytes);
}
