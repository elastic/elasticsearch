/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest.geoip;

import com.maxmind.db.NodeCache;
import com.maxmind.geoip2.model.AbstractResponse;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.function.Function;

/**
 * The in-memory cache for the geoip data. There should only be 1 instance of this class.
 * This cache differs from the maxmind's {@link NodeCache} such that this cache stores the deserialized Json objects to avoid the
 * cost of deserialization for each lookup (cached or not). This comes at slight expense of higher memory usage, but significant
 * reduction of CPU usage.
 */
final class GeoIpCache {

    /**
     * Internal-only sentinel object for recording that a result from the geoip database was null (i.e. there was no result). By caching
     * this no-result we can distinguish between something not being in the cache because we haven't searched for that data yet, versus
     * something not being in the cache because the data doesn't exist in the database.
     */
    // visible for testing
    static final AbstractResponse NO_RESULT = new AbstractResponse() {
        @Override
        public String toString() {
            return "AbstractResponse[NO_RESULT]";
        }
    };

    private final Cache<CacheKey, AbstractResponse> cache;

    // package private for testing
    GeoIpCache(long maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("geoip max cache size must be 0 or greater");
        }
        this.cache = CacheBuilder.<CacheKey, AbstractResponse>builder().setMaximumWeight(maxSize).build();
    }

    @SuppressWarnings("unchecked")
    <T extends AbstractResponse> T putIfAbsent(
        InetAddress ip,
        String databasePath,
        Function<InetAddress, AbstractResponse> retrieveFunction
    ) {
        // can't use cache.computeIfAbsent due to the elevated permissions for the jackson (run via the cache loader)
        CacheKey cacheKey = new CacheKey(ip, databasePath);
        // intentionally non-locking for simplicity...it's OK if we re-put the same key/value in the cache during a race condition.
        AbstractResponse response = cache.get(cacheKey);

        // populate the cache for this key, if necessary
        if (response == null) {
            response = retrieveFunction.apply(ip);
            // if the response from the database was null, then use the no-result sentinel value
            if (response == null) {
                response = NO_RESULT;
            }
            // store the result or no-result in the cache
            cache.put(cacheKey, response);
        }

        if (response == NO_RESULT) {
            return null; // the no-result sentinel is an internal detail, don't expose it
        } else {
            return (T) response;
        }
    }

    // only useful for testing
    AbstractResponse get(InetAddress ip, String databasePath) {
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
     * The key to use for the cache. Since this cache can span multiple geoip processors that all use different databases, the database
     * path is needed to be included in the cache key. For example, if we only used the IP address as the key the City and ASN the same
     * IP may be in both with different values and we need to cache both.
     */
    private record CacheKey(InetAddress ip, String databasePath) {}
}
