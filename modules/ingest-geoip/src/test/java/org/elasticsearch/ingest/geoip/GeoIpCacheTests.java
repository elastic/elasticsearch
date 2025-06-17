/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.model.AbstractResponse;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.geoip.stats.CacheStats;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class GeoIpCacheTests extends ESTestCase {

    public void testCachesAndEvictsResults() {
        GeoIpCache cache = new GeoIpCache(1);
        ProjectId projectId = randomProjectIdOrDefault();
        AbstractResponse response1 = mock(AbstractResponse.class);
        AbstractResponse response2 = mock(AbstractResponse.class);

        // add a key
        AbstractResponse cachedResponse = cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db", ip -> response1);
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

    public void testCachesNoResult() {
        GeoIpCache cache = new GeoIpCache(1);
        final AtomicInteger count = new AtomicInteger(0);
        Function<String, AbstractResponse> countAndReturnNull = (ip) -> {
            count.incrementAndGet();
            return null;
        };

        ProjectId projectId = randomProjectIdOrDefault();
        AbstractResponse response = cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db", countAndReturnNull);
        assertNull(response);
        assertNull(cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db", countAndReturnNull));
        assertEquals(1, count.get());

        // the cached value is not actually *null*, it's the NO_RESULT sentinel
        assertSame(GeoIpCache.NO_RESULT, cache.get(projectId, "127.0.0.1", "path/to/db"));
    }

    public void testCacheDoesNotCollideForDifferentDatabases() {
        GeoIpCache cache = new GeoIpCache(2);
        AbstractResponse response1 = mock(AbstractResponse.class);
        AbstractResponse response2 = mock(AbstractResponse.class);

        ProjectId projectId = randomProjectIdOrDefault();
        assertSame(response1, cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db1", ip -> response1));
        assertSame(response2, cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db2", ip -> response2));
        assertSame(response1, cache.get(projectId, "127.0.0.1", "path/to/db1"));
        assertSame(response2, cache.get(projectId, "127.0.0.1", "path/to/db2"));
    }

    public void testCacheDoesNotCollideForDifferentProjects() {
        GeoIpCache cache = new GeoIpCache(2);
        AbstractResponse response1 = mock(AbstractResponse.class);
        AbstractResponse response2 = mock(AbstractResponse.class);

        ProjectId projectId1 = randomUniqueProjectId();
        ProjectId projectId2 = randomUniqueProjectId();
        assertSame(response1, cache.putIfAbsent(projectId1, "127.0.0.1", "path/to/db1", ip -> response1));
        assertSame(response2, cache.putIfAbsent(projectId2, "127.0.0.1", "path/to/db1", ip -> response2));
        assertSame(response1, cache.get(projectId1, "127.0.0.1", "path/to/db1"));
        assertSame(response2, cache.get(projectId2, "127.0.0.1", "path/to/db1"));
    }

    public void testThrowsFunctionsException() {
        GeoIpCache cache = new GeoIpCache(1);
        ProjectId projectId = randomProjectIdOrDefault();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> cache.putIfAbsent(projectId, "127.0.0.1", "path/to/db", ip -> {
                throw new IllegalArgumentException("bad");
            })
        );
        assertEquals("bad", ex.getMessage());
    }

    public void testInvalidInit() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new GeoIpCache(-1));
        assertEquals("geoip max cache size must be 0 or greater", ex.getMessage());
    }

    public void testGetCacheStats() {
        final long maxCacheSize = 2;
        final AtomicLong testNanoTime = new AtomicLong(0);
        // We use a relative time provider that increments 1ms every time it is called. So each operation appears to take 1ms
        GeoIpCache cache = new GeoIpCache(maxCacheSize, () -> testNanoTime.addAndGet(TimeValue.timeValueMillis(1).getNanos()));
        ProjectId projectId = randomProjectIdOrDefault();
        AbstractResponse response = mock(AbstractResponse.class);
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
        GeoIpCache cache = new GeoIpCache(100);
        ProjectId projectId1 = randomUniqueProjectId();
        ProjectId projectId2 = randomUniqueProjectId();
        String databasePath1 = "path/to/db1";
        String databasePath2 = "path/to/db2";
        String ip1 = "127.0.0.1";
        String ip2 = "127.0.0.2";

        AbstractResponse response = mock(AbstractResponse.class);
        cache.putIfAbsent(projectId1, ip1, databasePath1, ip -> response); // cache miss
        cache.putIfAbsent(projectId1, ip2, databasePath1, ip -> response); // cache miss
        cache.putIfAbsent(projectId2, ip1, databasePath1, ip -> response); // cache miss
        cache.putIfAbsent(projectId1, ip1, databasePath2, ip -> response); // cache miss
        cache.purgeCacheEntriesForDatabase(projectId1, PathUtils.get(databasePath1));
        // should have purged entries for projectId1 and databasePath1...
        assertNull(cache.get(projectId1, ip1, databasePath1));
        assertNull(cache.get(projectId1, ip2, databasePath1));
        // ...but left the one for projectId2...
        assertSame(response, cache.get(projectId2, ip1, databasePath1));
        // ...and for databasePath2:
        assertSame(response, cache.get(projectId1, ip1, databasePath2));
    }
}
