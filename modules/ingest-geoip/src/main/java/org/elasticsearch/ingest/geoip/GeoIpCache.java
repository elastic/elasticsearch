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
import java.util.Objects;
import java.util.function.Function;

/**
 * The in-memory cache for the geoip data. There should only be 1 instance of this class..
 * This cache differs from the maxmind's {@link NodeCache} such that this cache stores the deserialized Json objects to avoid the
 * cost of deserialization for each lookup (cached or not). This comes at slight expense of higher memory usage, but significant
 * reduction of CPU usage.
 */
final class GeoIpCache {
    private final Cache<CacheKey, AbstractResponse> cache;

    //package private for testing
    GeoIpCache(long maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("geoip max cache size must be 0 or greater");
        }
        this.cache = CacheBuilder.<CacheKey, AbstractResponse>builder().setMaximumWeight(maxSize).build();
    }

    @SuppressWarnings("unchecked")
    <T extends AbstractResponse> T putIfAbsent(InetAddress ip,
                                               String databasePath,
                                               Function<InetAddress, AbstractResponse> retrieveFunction) {

        //can't use cache.computeIfAbsent due to the elevated permissions for the jackson (run via the cache loader)
        CacheKey cacheKey = new CacheKey(ip, databasePath);
        //intentionally non-locking for simplicity...it's OK if we re-put the same key/value in the cache during a race condition.
        AbstractResponse response = cache.get(cacheKey);
        if (response == null) {
            response = retrieveFunction.apply(ip);
            cache.put(cacheKey, response);
        }
        return (T) response;
    }

    //only useful for testing
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
    private static class CacheKey {

        private final InetAddress ip;
        private final String databasePath;

        private CacheKey(InetAddress ip, String databasePath) {
            this.ip = ip;
            this.databasePath = databasePath;
        }

        //generated
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(ip, cacheKey.ip) &&
                Objects.equals(databasePath, cacheKey.databasePath);
        }

        //generated
        @Override
        public int hashCode() {
            return Objects.hash(ip, databasePath);
        }
    }
}
