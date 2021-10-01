/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import java.util.Objects;

/**
 * A simple cache for enrich that uses {@link Cache}. There is one instance of this cache and
 * multiple enrich processors with different policies will use this cache.
 *
 * The key of the cache is based on the search request and the enrich index that will be used.
 * Search requests that enrich generates target the alias for an enrich policy, this class
 * resolves the alias to the actual enrich index and uses that for the cache key. This way
 * no stale entries will be returned if a policy execution happens and a new enrich index is created.
 *
 * There is no cleanup mechanism of stale entries in case a new enrich index is created
 * as part of a policy execution. This shouldn't be needed as cache entries for prior enrich
 * indices will be eventually evicted, because these entries will not end up being used. The
 * latest enrich index name will be used as cache key after an enrich policy execution.
 * (Also a cleanup mechanism also wouldn't be straightforward to implement,
 * since there is no easy check to see that an enrich index used as cache key no longer is the
 * current enrich index the enrich alias of an policy refers to. It would require checking
 * all cached entries on each cluster state update)
 */
public final class EnrichCache {

    private final Cache<CacheKey, SearchResponse> cache;
    private volatile Metadata metadata;

    EnrichCache(long maxSize) {
        this.cache = CacheBuilder.<CacheKey, SearchResponse>builder().setMaximumWeight(maxSize).build();
    }

    SearchResponse get(SearchRequest searchRequest) {
        String enrichIndex = getEnrichIndexKey(searchRequest);
        CacheKey cacheKey = new CacheKey(enrichIndex, searchRequest);

        return cache.get(cacheKey);
    }

    void put(SearchRequest searchRequest, SearchResponse searchResponse) {
        String enrichIndex = getEnrichIndexKey(searchRequest);
        CacheKey cacheKey = new CacheKey(enrichIndex, searchRequest);

        cache.put(cacheKey, searchResponse);
    }

    void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public EnrichStatsAction.Response.CacheStats getStats(String localNodeId) {
        Cache.CacheStats cacheStats = cache.stats();
        return new EnrichStatsAction.Response.CacheStats(
            localNodeId,
            cache.count(),
            cacheStats.getHits(),
            cacheStats.getMisses(),
            cacheStats.getEvictions()
        );
    }

    private String getEnrichIndexKey(SearchRequest searchRequest) {
        String alias = searchRequest.indices()[0];
        IndexAbstraction ia = metadata.getIndicesLookup().get(alias);
        return ia.getIndices().get(0).getIndex().getName();
    }

    private static class CacheKey {

        final String enrichIndex;
        final SearchRequest searchRequest;

        private CacheKey(String enrichIndex, SearchRequest searchRequest) {
            this.enrichIndex = enrichIndex;
            this.searchRequest = searchRequest;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return enrichIndex.equals(cacheKey.enrichIndex) && searchRequest.equals(cacheKey.searchRequest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(enrichIndex, searchRequest);
        }
    }

}
