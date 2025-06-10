/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.geoip.stats.CacheStats;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class GeoIpCacheTests extends ESTestCase {

    public void testCachesAndEvictsResults() {
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(1);
        GeoIpCache.CacheableValue response1 = mock(GeoIpCache.CacheableValue.class);
        GeoIpCache.CacheableValue response2 = mock(GeoIpCache.CacheableValue.class);

        // add a key
        GeoIpCache.CacheableValue cachedResponse = cache.putIfAbsent("127.0.0.1", "path/to/db", ip -> response1);
        assertSame(cachedResponse, response1);
        assertSame(cachedResponse, cache.putIfAbsent("127.0.0.1", "path/to/db", ip -> response1));
        assertSame(cachedResponse, cache.get("127.0.0.1", "path/to/db"));

        // evict old key by adding another value
        cachedResponse = cache.putIfAbsent("127.0.0.2", "path/to/db", ip -> response2);
        assertSame(cachedResponse, response2);
        assertSame(cachedResponse, cache.putIfAbsent("127.0.0.2", "path/to/db", ip -> response2));
        assertSame(cachedResponse, cache.get("127.0.0.2", "path/to/db"));
        assertNotSame(response1, cache.get("127.0.0.1", "path/to/db"));
    }

    public void testCachesNoResult() {
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(1);
        final AtomicInteger count = new AtomicInteger(0);
        Function<String, GeoIpCache.CacheableValue> countAndReturnNull = (ip) -> {
            count.incrementAndGet();
            return null;
        };

        GeoIpCache.CacheableValue response = cache.putIfAbsent("127.0.0.1", "path/to/db", countAndReturnNull);
        assertNull(response);
        assertNull(cache.putIfAbsent("127.0.0.1", "path/to/db", countAndReturnNull));
        assertEquals(1, count.get());

        // the cached value is not actually *null*, it's the NO_RESULT sentinel
        assertSame(GeoIpCache.NO_RESULT, cache.get("127.0.0.1", "path/to/db"));
    }

    public void testCacheKey() {
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(2);
        GeoIpCache.CacheableValue response1 = mock(GeoIpCache.CacheableValue.class);
        GeoIpCache.CacheableValue response2 = mock(GeoIpCache.CacheableValue.class);

        assertSame(response1, cache.putIfAbsent("127.0.0.1", "path/to/db1", ip -> response1));
        assertSame(response2, cache.putIfAbsent("127.0.0.1", "path/to/db2", ip -> response2));
        assertSame(response1, cache.get("127.0.0.1", "path/to/db1"));
        assertSame(response2, cache.get("127.0.0.1", "path/to/db2"));
    }

    public void testThrowsFunctionsException() {
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(1);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> cache.putIfAbsent("127.0.0.1", "path/to/db", ip -> {
                throw new IllegalArgumentException("bad");
            })
        );
        assertEquals("bad", ex.getMessage());
    }

    public void testInvalidInit() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> GeoIpCache.createGeoIpCacheWithMaxCount(-1));
        assertEquals("geoip max cache size must be 0 or greater", ex.getMessage());
    }

    public void testGetCacheStats() {
        final long maxCacheSize = 2;
        final AtomicLong testNanoTime = new AtomicLong(0);
        // We use a relative time provider that increments 1ms every time it is called. So each operation appears to take 1ms
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCountAndCustomTimeProvider(
            maxCacheSize,
            () -> testNanoTime.addAndGet(TimeValue.timeValueMillis(1).getNanos())
        );
        GeoIpCache.CacheableValue response = mock(GeoIpCache.CacheableValue.class);
        String databasePath = "path/to/db1";
        String key1 = "127.0.0.1";
        String key2 = "127.0.0.2";
        String key3 = "127.0.0.3";

        cache.putIfAbsent(key1, databasePath, ip -> response); // cache miss
        cache.putIfAbsent(key2, databasePath, ip -> response); // cache miss
        cache.putIfAbsent(key1, databasePath, ip -> response); // cache hit
        cache.putIfAbsent(key1, databasePath, ip -> response); // cache hit
        cache.putIfAbsent(key1, databasePath, ip -> response); // cache hit
        cache.putIfAbsent(key3, databasePath, ip -> response); // cache miss, key2 will be evicted
        cache.putIfAbsent(key2, databasePath, ip -> response); // cache miss, key1 will be evicted
        CacheStats cacheStats = cache.getCacheStats();
        assertThat(cacheStats.count(), equalTo(maxCacheSize));
        assertThat(cacheStats.hits(), equalTo(3L));
        assertThat(cacheStats.misses(), equalTo(4L));
        assertThat(cacheStats.evictions(), equalTo(2L));
        // There are 3 hits, each taking 1ms:
        assertThat(cacheStats.hitsTimeInMillis(), equalTo(3L));
        // There are 4 misses. Each is made up of a cache query, and a database query, each being 1ms:
        assertThat(cacheStats.missesTimeInMillis(), equalTo(8L));
    }
}
