/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.geoip.stats.CacheStats;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class GeoIpCacheTests extends ESTestCase {

    private record FakeResponse(long sizeInBytes) implements GeoIpCache.CacheableValue {}

    public void testCachesAndEvictsResults_maxCount() {
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(1);
        ProjectId projectId = randomProjectIdOrDefault();
        FakeResponse response1 = new FakeResponse(0);
        FakeResponse response2 = new FakeResponse(0);

        // add a key
        FakeResponse cachedResponse = cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db", ip -> response1);
        assertSame(cachedResponse, response1);
        assertSame(cachedResponse, cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db", ip -> response1));
        assertSame(cachedResponse, cache.get(projectId, "127.0.0.1", "path/to/db"));

        // evict old key by adding another value
        cachedResponse = cache.putIfAbsent(projectId, "127.0.0.2", "path/to/db", ip -> response2);
        assertSame(cachedResponse, response2);
        assertSame(cachedResponse, cache.putIfAbsent(projectId, "127.0.0.2", "path/to/db", ip -> response2));
        assertSame(cachedResponse, cache.get(projectId, "127.0.0.2", "path/to/db"));
        assertNotSame(response1, cache.get(projectId, "127.0.0.1", "path/to/db"));
    }

    public void testCachesAndEvictsResults_maxBytes() {
        ProjectId projectId = randomProjectIdOrDefault();
        String ip1 = "127.0.0.1";
        String databasePath1 = "path/to/db";
        FakeResponse response1 = new FakeResponse(111);
        String ip2 = "127.0.0.2";
        String databasePath2 = "path/to/db";
        FakeResponse response2 = new FakeResponse(222);
        long totalSize = GeoIpCache.keySizeInBytes(projectId, ip1, databasePath1) + GeoIpCache.keySizeInBytes(projectId, ip2, databasePath2)
            + response1.sizeInBytes() + response2.sizeInBytes();

        GeoIpCache justBigEnoughCache = GeoIpCache.createGeoIpCacheWithMaxBytes(ByteSizeValue.ofBytes(totalSize));
        justBigEnoughCache.putIfAbsent(projectId, ip1, databasePath1, ip -> response1);
        justBigEnoughCache.putIfAbsent(projectId, ip2, databasePath2, ip -> response2);
        // Cache is just big enough for both values:
        assertSame(response2, justBigEnoughCache.get(projectId, ip2, databasePath2));
        assertSame(response1, justBigEnoughCache.get(projectId, ip1, databasePath1));

        GeoIpCache justTooSmallCache = GeoIpCache.createGeoIpCacheWithMaxBytes(ByteSizeValue.ofBytes(totalSize - 1L));
        justTooSmallCache.putIfAbsent(projectId, ip1, databasePath1, ip -> response1);
        justTooSmallCache.putIfAbsent(projectId, ip2, databasePath2, ip -> response2);
        // Cache is just too small for both values, so the older one should have been evicted:
        assertSame(response2, justTooSmallCache.get(projectId, ip2, databasePath2));
        assertNull(justTooSmallCache.get(projectId, ip1, databasePath1));
    }

    public void testCachesNoResult() {
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(1);
        final AtomicInteger count = new AtomicInteger(0);
        Function<String, FakeResponse> countAndReturnNull = (ip) -> {
            count.incrementAndGet();
            return null;
        };

        ProjectId projectId = randomProjectIdOrDefault();
        FakeResponse response = cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db", countAndReturnNull);
        assertNull(response);
        assertNull(cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db", countAndReturnNull));
        assertEquals(1, count.get());

        // the cached value is not actually *null*, it's the NO_RESULT sentinel
        assertSame(GeoIpCache.NO_RESULT, cache.get(projectId, "127.0.0.1", "path/to/db"));
    }

    public void testCacheDoesNotCollideForDifferentDatabases() {
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(2);
        FakeResponse response1 = new FakeResponse(111);
        FakeResponse response2 = new FakeResponse(222);

        ProjectId projectId = randomProjectIdOrDefault();
        assertSame(response1, cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db1", ip -> response1));
        assertSame(response2, cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db2", ip -> response2));
        assertSame(response1, cache.get(projectId, "127.0.0.1", "path/to/db1"));
        assertSame(response2, cache.get(projectId, "127.0.0.1", "path/to/db2"));
    }

    public void testCacheDoesNotCollideForDifferentProjects() {
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(2);
        FakeResponse response1 = new FakeResponse(111);
        FakeResponse response2 = new FakeResponse(222);

        ProjectId projectId1 = randomUniqueProjectId();
        ProjectId projectId2 = randomUniqueProjectId();
        assertSame(response1, cache.putIfAbsent(projectId1, "127.0.0.1", "path/to/db1", ip -> response1));
        assertSame(response2, cache.putIfAbsent(projectId2, "127.0.0.1", "path/to/db1", ip -> response2));
        assertSame(response1, cache.get(projectId1, "127.0.0.1", "path/to/db1"));
        assertSame(response2, cache.get(projectId2, "127.0.0.1", "path/to/db1"));
    }

    public void testThrowsFunctionsException() {
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(1);
        ProjectId projectId = randomProjectIdOrDefault();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db", ip -> {
                throw new IllegalArgumentException("bad");
            })
        );
        assertEquals("bad", ex.getMessage());
    }

    public void testInvalidInit_maxCount() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> GeoIpCache.createGeoIpCacheWithMaxCount(-1));
        assertEquals("geoip max cache size must be 0 or greater", ex.getMessage());
    }

    public void testInvalidInit_maxBytes() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> GeoIpCache.createGeoIpCacheWithMaxBytes(ByteSizeValue.MINUS_ONE)
        );
        assertEquals("geoip max cache size in bytes must be 0 or greater", ex.getMessage());
    }

    public void testGetCacheStats() {
        final long maxCacheSize = 2;
        final AtomicLong testNanoTime = new AtomicLong(0);
        // We use a relative time provider that increments 1ms every time it is called. So each operation appears to take 1ms
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCountAndCustomTimeProvider(
            maxCacheSize,
            () -> testNanoTime.addAndGet(TimeValue.timeValueMillis(1).getNanos())
        );
        ProjectId projectId = randomProjectIdOrDefault();
        FakeResponse response = new FakeResponse(0);
        String databasePath = "path/to/db1";
        String key1 = "127.0.0.1";
        String key2 = "127.0.0.2";
        String key3 = "127.0.0.3";

        cache.putIfAbsent(projectId, key1, databasePath, ip -> response); // cache miss
        cache.putIfAbsent(projectId, key2, databasePath, ip -> response); // cache miss
        cache.putIfAbsent(projectId, key1, databasePath, ip -> response); // cache hit
        cache.putIfAbsent(projectId, key1, databasePath, ip -> response); // cache hit
        cache.putIfAbsent(projectId, key1, databasePath, ip -> response); // cache hit
        cache.putIfAbsent(projectId, key3, databasePath, ip -> response); // cache miss, key2 will be evicted
        cache.putIfAbsent(projectId, key2, databasePath, ip -> response); // cache miss, key1 will be evicted
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

    public void testPurgeCacheEntriesForDatabase() {
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(100);
        ProjectId projectId1 = randomUniqueProjectId();
        ProjectId projectId2 = randomUniqueProjectId();
        // Turn the path strings into Paths to ensure that we always use the canonical string representation (this string literal does not
        // round-trip when converting to a Path and back again on Windows):
        Path databasePath1 = PathUtils.get("path/to/db1");
        Path databasePath2 = PathUtils.get("path/to/db2");
        String ip1 = "127.0.0.1";
        String ip2 = "127.0.0.2";

        FakeResponse response = new FakeResponse(111);
        cache.putIfAbsent(projectId1, ip1, databasePath1.toString(), ip -> response); // cache miss
        cache.putIfAbsent(projectId1, ip2, databasePath1.toString(), ip -> response); // cache miss
        cache.putIfAbsent(projectId2, ip1, databasePath1.toString(), ip -> response); // cache miss
        cache.putIfAbsent(projectId1, ip1, databasePath2.toString(), ip -> response); // cache miss
        cache.purgeCacheEntriesForDatabase(projectId1, databasePath1);
        // should have purged entries for projectId1 and databasePath1...
        assertNull(cache.get(projectId1, ip1, databasePath1.toString()));
        assertNull(cache.get(projectId1, ip2, databasePath1.toString()));
        // ...but left the one for projectId2...
        assertSame(response, cache.get(projectId2, ip1, databasePath1.toString()));
        // ...and for databasePath2:
        assertSame(response, cache.get(projectId1, ip1, databasePath2.toString()));
    }
}
