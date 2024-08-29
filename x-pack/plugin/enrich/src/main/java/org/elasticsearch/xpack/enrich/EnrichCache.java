/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongBiFunction;

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

    private final Cache<CacheKey, CacheValue> cache;
    private final LongSupplier relativeNanoTimeProvider;
    private final AtomicLong hitsTimeInNanos = new AtomicLong(0);
    private final AtomicLong missesTimeInNanos = new AtomicLong(0);
    private final AtomicLong sizeInBytes = new AtomicLong(0);
    private volatile Metadata metadata;

    EnrichCache(long maxSize) {
        this(maxSize, System::nanoTime);
    }

    EnrichCache(ByteSizeValue maxByteSize) {
        this(maxByteSize, System::nanoTime);
    }

    // non-private for unit testing only
    EnrichCache(long maxSize, LongSupplier relativeNanoTimeProvider) {
        this.relativeNanoTimeProvider = relativeNanoTimeProvider;
        this.cache = createCache(maxSize, null);
    }

    EnrichCache(ByteSizeValue maxByteSize, LongSupplier relativeNanoTimeProvider) {
        this.relativeNanoTimeProvider = relativeNanoTimeProvider;
        this.cache = createCache(maxByteSize.getBytes(), (key, value) -> value.sizeInBytes);
    }

    private Cache<CacheKey, CacheValue> createCache(long maxWeight, ToLongBiFunction<CacheKey, CacheValue> weigher) {
        var builder = CacheBuilder.<CacheKey, CacheValue>builder().setMaximumWeight(maxWeight).removalListener(notification -> {
            sizeInBytes.getAndAdd(-1 * notification.getValue().sizeInBytes);
        });
        if (weigher != null) {
            builder.weigher(weigher);
        }
        return builder.build();
    }

    /**
     * This method notifies the given listener of the value in this cache for the given searchRequest. If there is no value in the cache
     * for the searchRequest, then the new cache value is computed using searchResponseFetcher.
     * @param searchRequest The key for the cache request
     * @param searchResponseFetcher The function used to compute the value to be put in the cache, if there is no value in the cache already
     * @param listener A listener to be notified of the value in the cache
     */
    public void computeIfAbsent(
        SearchRequest searchRequest,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> searchResponseFetcher,
        ActionListener<List<Map<?, ?>>> listener
    ) {
        // intentionally non-locking for simplicity...it's OK if we re-put the same key/value in the cache during a race condition.
        long cacheStart = relativeNanoTimeProvider.getAsLong();
        List<Map<?, ?>> response = get(searchRequest);
        long cacheRequestTime = relativeNanoTimeProvider.getAsLong() - cacheStart;
        if (response != null) {
            hitsTimeInNanos.addAndGet(cacheRequestTime);
            listener.onResponse(response);
        } else {
            final long retrieveStart = relativeNanoTimeProvider.getAsLong();
            searchResponseFetcher.accept(searchRequest, ActionListener.wrap(resp -> {
                CacheValue value = toCacheValue(resp);
                put(searchRequest, value);
                List<Map<?, ?>> copy = deepCopy(value.hits, false);
                long databaseQueryAndCachePutTime = relativeNanoTimeProvider.getAsLong() - retrieveStart;
                missesTimeInNanos.addAndGet(cacheRequestTime + databaseQueryAndCachePutTime);
                listener.onResponse(copy);
            }, listener::onFailure));
        }
    }

    // non-private for unit testing only
    List<Map<?, ?>> get(SearchRequest searchRequest) {
        String enrichIndex = getEnrichIndexKey(searchRequest);
        CacheKey cacheKey = new CacheKey(enrichIndex, searchRequest);

        CacheValue response = cache.get(cacheKey);
        if (response != null) {
            return deepCopy(response.hits, false);
        } else {
            return null;
        }
    }

    // non-private for unit testing only
    void put(SearchRequest searchRequest, CacheValue cacheValue) {
        String enrichIndex = getEnrichIndexKey(searchRequest);
        CacheKey cacheKey = new CacheKey(enrichIndex, searchRequest);

        cache.put(cacheKey, cacheValue);
        sizeInBytes.addAndGet(cacheValue.sizeInBytes);
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
            cacheStats.getEvictions(),
            TimeValue.nsecToMSec(hitsTimeInNanos.get()),
            TimeValue.nsecToMSec(missesTimeInNanos.get()),
            sizeInBytes.get()
        );
    }

    private String getEnrichIndexKey(SearchRequest searchRequest) {
        String alias = searchRequest.indices()[0];
        IndexAbstraction ia = metadata.getIndicesLookup().get(alias);
        if (ia == null) {
            throw new IndexNotFoundException("no generated enrich index [" + alias + "]");
        }
        return ia.getIndices().get(0).getName();
    }

    static CacheValue toCacheValue(SearchResponse response) {
        List<Map<?, ?>> result = new ArrayList<>(response.getHits().getHits().length);
        long size = 0;
        for (SearchHit hit : response.getHits()) {
            result.add(deepCopy(hit.getSourceAsMap(), true));
            size += hit.getSourceRef() != null ? hit.getSourceRef().ramBytesUsed() : 0;
        }
        return new CacheValue(Collections.unmodifiableList(result), size);
    }

    @SuppressWarnings("unchecked")
    static <T> T deepCopy(T value, boolean unmodifiable) {
        return (T) innerDeepCopy(value, unmodifiable);
    }

    private static Object innerDeepCopy(Object value, boolean unmodifiable) {
        if (value instanceof Map<?, ?> mapValue) {
            Map<Object, Object> copy = Maps.newMapWithExpectedSize(mapValue.size());
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                copy.put(entry.getKey(), innerDeepCopy(entry.getValue(), unmodifiable));
            }
            return unmodifiable ? Collections.unmodifiableMap(copy) : copy;
        } else if (value instanceof List<?> listValue) {
            List<Object> copy = new ArrayList<>(listValue.size());
            for (Object itemValue : listValue) {
                copy.add(innerDeepCopy(itemValue, unmodifiable));
            }
            return unmodifiable ? Collections.unmodifiableList(copy) : copy;
        } else if (value instanceof byte[] bytes) {
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

    // Visibility for testing
    record CacheValue(List<Map<?, ?>> hits, Long sizeInBytes) {}
}
