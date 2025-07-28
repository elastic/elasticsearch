/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongBiFunction;

/**
 * A simple cache for enrich that uses {@link Cache}. There is one instance of this cache and
 * multiple enrich processors with different policies will use this cache.
 * <p>
 * There is no cleanup mechanism of stale entries in case a new enrich index is created
 * as part of a policy execution. This shouldn't be needed as cache entries for prior enrich
 * indices will be eventually evicted, because these entries will not end up being used. The
 * latest enrich index name will be used as cache key after an enrich policy execution.
 * (Also a cleanup mechanism wouldn't be straightforward to implement,
 * since there is no easy check to see that an enrich index used as cache key no longer is the
 * current enrich index that the enrich alias of a policy refers to. It would require checking
 * all cached entries on each cluster state update)
 */
public final class EnrichCache {

    private static final CacheValue EMPTY_CACHE_VALUE = new CacheValue(List.of(), CacheKey.CACHE_KEY_SIZE);

    private final Cache<CacheKey, CacheValue> cache;
    private final LongSupplier relativeNanoTimeProvider;
    private final AtomicLong hitsTimeInNanos = new AtomicLong(0);
    private final AtomicLong missesTimeInNanos = new AtomicLong(0);
    private final AtomicLong sizeInBytes = new AtomicLong(0);

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
     * This method notifies the given listener of the value in this cache for the given search parameters. If there is no value in the cache
     * for these search parameters, then the new cache value is computed using searchResponseFetcher.
     *
     * @param projectId             The ID of the project
     * @param enrichIndex           The enrich index from which the results will be retrieved
     * @param lookupValue           The value that will be used in the search
     * @param maxMatches            The max number of matches that the search will return
     * @param searchResponseFetcher The function used to compute the value to be put in the cache, if there is no value in the cache already
     * @param listener              A listener to be notified of the value in the cache
     */
    public void computeIfAbsent(
        ProjectId projectId,
        String enrichIndex,
        Object lookupValue,
        int maxMatches,
        Consumer<ActionListener<SearchResponse>> searchResponseFetcher,
        ActionListener<List<Map<?, ?>>> listener
    ) {
        // intentionally non-locking for simplicity...it's OK if we re-put the same key/value in the cache during a race condition.
        long cacheStart = relativeNanoTimeProvider.getAsLong();
        var cacheKey = new CacheKey(projectId, enrichIndex, lookupValue, maxMatches);
        List<Map<?, ?>> response = get(cacheKey);
        long cacheRequestTime = relativeNanoTimeProvider.getAsLong() - cacheStart;
        if (response != null) {
            hitsTimeInNanos.addAndGet(cacheRequestTime);
            listener.onResponse(response);
        } else {
            final long retrieveStart = relativeNanoTimeProvider.getAsLong();
            searchResponseFetcher.accept(ActionListener.wrap(resp -> {
                CacheValue cacheValue = toCacheValue(resp);
                put(cacheKey, cacheValue);
                List<Map<?, ?>> copy = deepCopy(cacheValue.hits, false);
                long databaseQueryAndCachePutTime = relativeNanoTimeProvider.getAsLong() - retrieveStart;
                missesTimeInNanos.addAndGet(cacheRequestTime + databaseQueryAndCachePutTime);
                listener.onResponse(copy);
            }, listener::onFailure));
        }
    }

    // non-private for unit testing only
    List<Map<?, ?>> get(CacheKey cacheKey) {
        CacheValue response = cache.get(cacheKey);
        if (response != null) {
            return deepCopy(response.hits, false);
        } else {
            return null;
        }
    }

    // non-private for unit testing only
    void put(CacheKey cacheKey, CacheValue cacheValue) {
        cache.put(cacheKey, cacheValue);
        sizeInBytes.addAndGet(cacheValue.sizeInBytes);
    }

    public EnrichStatsAction.Response.CacheStats getStats(String localNodeId) {
        Cache.Stats cacheStats = cache.stats();
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

    static CacheValue toCacheValue(SearchResponse response) {
        if (response.getHits().getHits().length == 0) {
            return EMPTY_CACHE_VALUE;
        }
        List<Map<?, ?>> result = new ArrayList<>(response.getHits().getHits().length);
        // Include the size of the cache key.
        long size = CacheKey.CACHE_KEY_SIZE;
        for (SearchHit hit : response.getHits()) {
            // There is a cost of decompressing source here plus caching it.
            // We do it first so we don't decompress it twice.
            size += hit.getSourceRef() != null ? hit.getSourceRef().ramBytesUsed() : 0;
            // Do we need deep copy here, we are creating a modifiable map already?
            result.add(deepCopy(hit.getSourceAsMap(), true));
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

    /**
     * The cache key consists of the (variable) parameters that are used to construct a search request for the enrich lookup. We define a
     * custom record to group these fields to avoid constructing and storing the much larger
     * {@link org.elasticsearch.action.search.SearchRequest}.
     *
     * @param enrichIndex The enrich <i>index</i> (i.e. not the alias, but the concrete index that the alias points to)
     * @param lookupValue The value that is used to find matches in the enrich index
     * @param maxMatches  The max number of matches that the enrich lookup should return. This changes the size of the search response and
     *                    should thus be included in the cache key
     */
    // Visibility for testing
    record CacheKey(ProjectId projectId, String enrichIndex, Object lookupValue, int maxMatches) {
        /**
         * In reality, the size in bytes of the cache key is a function of the {@link CacheKey#lookupValue} field plus some constant for
         * the object itself, the string reference for the enrich index (but not the string itself because it's taken from the metadata),
         * and the integer for the max number of matches. However, by defining a static cache key size, we can make the
         * {@link EnrichCache#EMPTY_CACHE_VALUE} static as well, which allows us to avoid having to instantiate new cache values for
         * empty results and thus save some heap space.
         */
        private static final long CACHE_KEY_SIZE = 256L;
    }

    // Visibility for testing
    record CacheValue(List<Map<?, ?>> hits, Long sizeInBytes) {}
}
