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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private final Cache<CacheKey, List<Map<?, ?>>> cache;
    private volatile Metadata metadata;

    EnrichCache(long maxSize) {
        this.cache = CacheBuilder.<CacheKey, List<Map<?, ?>>>builder().setMaximumWeight(maxSize).build();
    }

    List<Map<?, ?>> get(SearchRequest searchRequest) {
        String enrichIndex = getEnrichIndexKey(searchRequest);
        CacheKey cacheKey = new CacheKey(enrichIndex, searchRequest);

        List<Map<?, ?>> response = cache.get(cacheKey);
        if (response != null) {
            return deepCopy(response, false);
        } else {
            return null;
        }
    }

    void put(SearchRequest searchRequest, List<Map<?, ?>> response) {
        String enrichIndex = getEnrichIndexKey(searchRequest);
        CacheKey cacheKey = new CacheKey(enrichIndex, searchRequest);

        cache.put(cacheKey, response);
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
        return ia.getIndices().get(0).getName();
    }

    List<Map<?, ?>> toCacheValue(SearchResponse response) {
        List<Map<?, ?>> result = new ArrayList<>(response.getHits().getHits().length);
        for (SearchHit hit : response.getHits()) {
            result.add(deepCopy(hit.getSourceAsMap(), true));
        }
        return Collections.unmodifiableList(result);
    }

    @SuppressWarnings("unchecked")
    static <T> T deepCopy(T value, boolean unmodifiable) {
        return (T) innerDeepCopy(value, unmodifiable);
    }

    private static Object innerDeepCopy(Object value, boolean unmodifiable) {
        if (value instanceof Map<?, ?>) {
            Map<?, ?> mapValue = (Map<?, ?>) value;
            Map<Object, Object> copy = new HashMap<>(mapValue.size());
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                copy.put(entry.getKey(), innerDeepCopy(entry.getValue(), unmodifiable));
            }
            return unmodifiable ? Collections.unmodifiableMap(copy) : copy;
        } else if (value instanceof List<?>) {
            List<?> listValue = (List<?>) value;
            List<Object> copy = new ArrayList<>(listValue.size());
            for (Object itemValue : listValue) {
                copy.add(innerDeepCopy(itemValue, unmodifiable));
            }
            return unmodifiable ? Collections.unmodifiableList(copy) : copy;
        } else if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            return Arrays.copyOf(bytes, bytes.length);
        } else if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        } else {
            throw new IllegalArgumentException("unexpected value type [" + value.getClass() + "]");
        }
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
