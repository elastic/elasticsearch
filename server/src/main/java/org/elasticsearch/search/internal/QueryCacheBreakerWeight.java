/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.indices.IndicesQueryCache;

/**
 * Transparent {@link Weight} wrapper that {@link ContextIndexSearcher#createWeight} puts inside the query cache's own
 * wrapper, so that the cache can reserve the heap of an entry it is about to build on the request circuit breaker.
 * The reservation goes through {@link ContextIndexSearcher#chargeLeaf} like the other execution charges
 * ({@link PointRangeBreakerWeight}, {@link MultiTermBreakerWeight}): it is attributed to the leaf, or to the
 * {@link LeafExecutionScope} bound to the thread, and released when the leaf is done, which is how long the search
 * holds the entry whatever the cache does with it. A refused reservation is reported rather than thrown: the cache
 * then skips the entry and the leaf runs uncached, so a search is slowed down rather than failed by a full cache
 * population, and only trips if the breaker is exhausted elsewhere.
 */
final class QueryCacheBreakerWeight extends IndicesQueryCache.CachePopulationAccountingWeight {

    static final String BREAKER_LABEL = "querycache-population";

    private final ContextIndexSearcher searcher;

    QueryCacheBreakerWeight(ContextIndexSearcher searcher, Weight in) {
        super(in);
        this.searcher = searcher;
    }

    @Override
    public boolean chargeCachePopulation(LeafReaderContext leaf, long bytes) {
        try {
            searcher.chargeLeaf(leaf, bytes, BREAKER_LABEL);
            return true;
        } catch (CircuitBreakingException e) {
            return false;
        }
    }
}
