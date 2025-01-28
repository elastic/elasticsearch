/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest.geoip;

import com.maxmind.db.NodeCache;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.geoip.stats.CacheStats;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * The in-memory cache for the geoip data. There should only be 1 instance of this class.
 * This cache differs from the maxmind's {@link NodeCache} such that this cache stores the deserialized Json objects to avoid the
 * cost of deserialization for each lookup (cached or not). This comes at slight expense of higher memory usage, but significant
 * reduction of CPU usage.
 */
public final class GeoIpCache {

    /**
     * Internal-only sentinel object for recording that a result from the geoip database was null (i.e. there was no result). By caching
     * this no-result we can distinguish between something not being in the cache because we haven't searched for that data yet, versus
     * something not being in the cache because the data doesn't exist in the database.
     */
    // visible for testing
    static final Object NO_RESULT = new Object() {
        @Override
        public String toString() {
            return "NO_RESULT";
        }
    };

    private final LongSupplier relativeNanoTimeProvider;
    private final Cache<CacheKey, Object> cache;
    private final AtomicLong hitsTimeInNanos = new AtomicLong(0);
    private final AtomicLong missesTimeInNanos = new AtomicLong(0);

    // package private for testing
    GeoIpCache(long maxSize, LongSupplier relativeNanoTimeProvider) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("geoip max cache size must be 0 or greater");
        }
        this.relativeNanoTimeProvider = relativeNanoTimeProvider;
        this.cache = CacheBuilder.<CacheKey, Object>builder().setMaximumWeight(maxSize).build();
    }

    GeoIpCache(long maxSize) {
        this(maxSize, System::nanoTime);
    }

    @SuppressWarnings("unchecked")
    <RESPONSE> RESPONSE putIfAbsent(String ip, String databasePath, Function<String, RESPONSE> retrieveFunction) {
        // can't use cache.computeIfAbsent due to the elevated permissions for the jackson (run via the cache loader)
        CacheKey cacheKey = new CacheKey(ip, databasePath);
        long cacheStart = relativeNanoTimeProvider.getAsLong();
        // intentionally non-locking for simplicity...it's OK if we re-put the same key/value in the cache during a race condition.
        Object response = cache.get(cacheKey);
        long cacheRequestTime = relativeNanoTimeProvider.getAsLong() - cacheStart;

        // populate the cache for this key, if necessary
        if (response == null) {
            long retrieveStart = relativeNanoTimeProvider.getAsLong();
            response = retrieveFunction.apply(ip);
            // if the response from the database was null, then use the no-result sentinel value
            if (response == null) {
                response = NO_RESULT;
            }
            // store the result or no-result in the cache
            cache.put(cacheKey, response);
            long databaseRequestAndCachePutTime = relativeNanoTimeProvider.getAsLong() - retrieveStart;
            missesTimeInNanos.addAndGet(cacheRequestTime + databaseRequestAndCachePutTime);
        } else {
            hitsTimeInNanos.addAndGet(cacheRequestTime);
        }

        if (response == NO_RESULT) {
            return null; // the no-result sentinel is an internal detail, don't expose it
        } else {
            return (RESPONSE) response;
        }
    }

    // only useful for testing
    Object get(String ip, String databasePath) {
        CacheKey cacheKey = new CacheKey(ip, databasePath);
        return cache.get(cacheKey);
    }

    public int purgeCacheEntriesForDatabase(Path databaseFile) {
        String databasePath = databaseFile.toString();
        int counter = 0;
        for (CacheKey key : cache.keys()) {
            if (key.databasePath.equals(databasePath)) {
                cache.invalidate(key);
                counter++;
            }
        }
        return counter;
    }

    public int count() {
        return cache.count();
    }

    /**
     * Returns stats about this cache as of this moment. There is no guarantee that the counts reconcile (for example hits + misses = count)
     * because no locking is performed when requesting these stats.
     * @return Current stats about this cache
     */
    public CacheStats getCacheStats() {
        Cache.CacheStats stats = cache.stats();
        return new CacheStats(
            cache.count(),
            stats.getHits(),
            stats.getMisses(),
            stats.getEvictions(),
            TimeValue.nsecToMSec(hitsTimeInNanos.get()),
            TimeValue.nsecToMSec(missesTimeInNanos.get())
        );
    }

    /**
     * The key to use for the cache. Since this cache can span multiple geoip processors that all use different databases, the database
     * path is needed to be included in the cache key. For example, if we only used the IP address as the key the City and ASN the same
     * IP may be in both with different values and we need to cache both.
     */
    private record CacheKey(String ip, String databasePath) {}
}
