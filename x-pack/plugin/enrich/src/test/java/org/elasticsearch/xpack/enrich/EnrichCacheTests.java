/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class EnrichCacheTests extends ESTestCase {

    public void testCaching() {
        // Emulated search requests that an enrich processor could generate:
        // (two unique searches for two enrich policies)
        var projectId = randomProjectIdOrDefault();
        var cacheKey1 = new EnrichCache.CacheKey("policy1-1", projectId, "1", 1);
        var cacheKey2 = new EnrichCache.CacheKey("policy1-1", projectId, "2", 1);
        var cacheKey3 = new EnrichCache.CacheKey("policy2-1", projectId, "1", 1);
        var cacheKey4 = new EnrichCache.CacheKey("policy2-1", projectId, "2", 1);
        // Emulated search response (content doesn't matter, since it isn't used, it just a cache entry)
        EnrichCache.CacheValue searchResponse = new EnrichCache.CacheValue(List.of(Map.of("test", "entry")), 1L);

        EnrichCache enrichCache = new EnrichCache(3);
        enrichCache.put(cacheKey1, searchResponse);
        enrichCache.put(cacheKey2, searchResponse);
        enrichCache.put(cacheKey3, searchResponse);
        var cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.count(), equalTo(3L));
        assertThat(cacheStats.hits(), equalTo(0L));
        assertThat(cacheStats.misses(), equalTo(0L));
        assertThat(cacheStats.evictions(), equalTo(0L));
        assertThat(cacheStats.cacheSizeInBytes(), equalTo(3L));

        assertThat(enrichCache.get(cacheKey1), notNullValue());
        assertThat(enrichCache.get(cacheKey2), notNullValue());
        assertThat(enrichCache.get(cacheKey3), notNullValue());
        assertThat(enrichCache.get(cacheKey4), nullValue());
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.count(), equalTo(3L));
        assertThat(cacheStats.hits(), equalTo(3L));
        assertThat(cacheStats.misses(), equalTo(1L));
        assertThat(cacheStats.evictions(), equalTo(0L));
        assertThat(cacheStats.cacheSizeInBytes(), equalTo(3L));

        enrichCache.put(cacheKey4, searchResponse);
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.count(), equalTo(3L));
        assertThat(cacheStats.hits(), equalTo(3L));
        assertThat(cacheStats.misses(), equalTo(1L));
        assertThat(cacheStats.evictions(), equalTo(1L));
        assertThat(cacheStats.cacheSizeInBytes(), equalTo(3L));

        cacheKey1 = new EnrichCache.CacheKey("policy1-2", projectId, "1", 1);
        cacheKey2 = new EnrichCache.CacheKey("policy1-2", projectId, "2", 1);
        cacheKey3 = new EnrichCache.CacheKey("policy2-2", projectId, "1", 1);
        cacheKey4 = new EnrichCache.CacheKey("policy2-2", projectId, "2", 1);

        // Because enrich index has changed, cache can't serve cached entries
        assertThat(enrichCache.get(cacheKey1), nullValue());
        assertThat(enrichCache.get(cacheKey2), nullValue());
        assertThat(enrichCache.get(cacheKey3), nullValue());
        assertThat(enrichCache.get(cacheKey4), nullValue());

        // Add new entries using new enrich index name as key
        enrichCache.put(cacheKey1, searchResponse);
        enrichCache.put(cacheKey2, searchResponse);
        enrichCache.put(cacheKey3, searchResponse);

        // Entries can now be served:
        assertThat(enrichCache.get(cacheKey1), notNullValue());
        assertThat(enrichCache.get(cacheKey2), notNullValue());
        assertThat(enrichCache.get(cacheKey3), notNullValue());
        assertThat(enrichCache.get(cacheKey4), nullValue());
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.count(), equalTo(3L));
        assertThat(cacheStats.hits(), equalTo(6L));
        assertThat(cacheStats.misses(), equalTo(6L));
        assertThat(cacheStats.evictions(), equalTo(4L));
        assertThat(cacheStats.cacheSizeInBytes(), equalTo(3L));
    }

    public void testComputeIfAbsent() throws InterruptedException {
        // Emulated search requests that an enrich processor could generate:
        // (two unique searches for two enrich policies)
        final List<Map<String, ?>> searchResponseMap = List.of(
            Map.of("key1", "value1", "key2", "value2"),
            Map.of("key3", "value3", "key4", "value4")
        );
        final AtomicLong testNanoTime = new AtomicLong(0);
        // We use a relative time provider that increments 1ms every time it is called. So each operation appears to take 1ms
        EnrichCache enrichCache = new EnrichCache(3, () -> testNanoTime.addAndGet(TimeValue.timeValueMillis(1).getNanos()));

        ProjectId projectId = randomProjectIdOrDefault();
        long expectedMisses = 0L;
        {
            // Do initial computeIfAbsent, assert that it is a cache miss and the search is performed:
            CountDownLatch queriedDatabaseLatch = new CountDownLatch(1);
            CountDownLatch notifiedOfResultLatch = new CountDownLatch(1);
            enrichCache.computeIfAbsent("policy1-1", projectId, "1", 1, (searchResponseActionListener) -> {
                SearchResponse searchResponse = convertToSearchResponse(searchResponseMap);
                searchResponseActionListener.onResponse(searchResponse);
                searchResponse.decRef();
                queriedDatabaseLatch.countDown();
            }, assertNoFailureListener(response -> {
                assertThat(response, equalTo(searchResponseMap));
                notifiedOfResultLatch.countDown();
            }));
            assertThat(queriedDatabaseLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            assertThat(notifiedOfResultLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            EnrichStatsAction.Response.CacheStats cacheStats = enrichCache.getStats(randomAlphaOfLength(10));
            assertThat(cacheStats.count(), equalTo(1L));
            assertThat(cacheStats.hits(), equalTo(0L));
            assertThat(cacheStats.misses(), equalTo(++expectedMisses));
            assertThat(cacheStats.evictions(), equalTo(0L));
            assertThat(cacheStats.hitsTimeInMillis(), equalTo(0L));
            assertThat(cacheStats.missesTimeInMillis(), equalTo(2L)); // cache query and enrich query + cache put
        }

        {
            // Do the same call, assert that it is a cache hit and no search is performed:
            CountDownLatch notifiedOfResultLatch = new CountDownLatch(1);
            enrichCache.computeIfAbsent("policy1-1", projectId, "1", 1, (searchResponseActionListener) -> {
                fail("Expected no call to the database because item should have been in the cache");
            }, assertNoFailureListener(r -> notifiedOfResultLatch.countDown()));
            assertThat(notifiedOfResultLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            EnrichStatsAction.Response.CacheStats cacheStats = enrichCache.getStats(randomAlphaOfLength(10));
            assertThat(cacheStats.count(), equalTo(1L));
            assertThat(cacheStats.hits(), equalTo(1L));
            assertThat(cacheStats.misses(), equalTo(expectedMisses));
            assertThat(cacheStats.evictions(), equalTo(0L));
            assertThat(cacheStats.hitsTimeInMillis(), equalTo(1L));
            assertThat(cacheStats.missesTimeInMillis(), equalTo(2L));
        }

        {
            // Do a computeIfAbsent with a different index, assert that it is a cache miss and the search is performed:
            CountDownLatch queriedDatabaseLatch = new CountDownLatch(1);
            CountDownLatch notifiedOfResultLatch = new CountDownLatch(1);
            enrichCache.computeIfAbsent("policy1-2", projectId, "1", 1, (searchResponseActionListener) -> {
                SearchResponse searchResponse = convertToSearchResponse(searchResponseMap);
                searchResponseActionListener.onResponse(searchResponse);
                searchResponse.decRef();
                queriedDatabaseLatch.countDown();
            }, assertNoFailureListener(response -> {
                assertThat(response, equalTo(searchResponseMap));
                notifiedOfResultLatch.countDown();
            }));
            assertThat(queriedDatabaseLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            assertThat(notifiedOfResultLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            EnrichStatsAction.Response.CacheStats cacheStats = enrichCache.getStats(randomAlphaOfLength(10));
            assertThat(cacheStats.misses(), equalTo(++expectedMisses));
        }

        {
            // Do a computeIfAbsent with a different project, assert that it is a cache miss and the search is performed:
            CountDownLatch queriedDatabaseLatch = new CountDownLatch(1);
            CountDownLatch notifiedOfResultLatch = new CountDownLatch(1);
            enrichCache.computeIfAbsent("policy1-1", randomUniqueProjectId(), "1", 1, (searchResponseActionListener) -> {
                SearchResponse searchResponse = convertToSearchResponse(searchResponseMap);
                searchResponseActionListener.onResponse(searchResponse);
                searchResponse.decRef();
                queriedDatabaseLatch.countDown();
            }, assertNoFailureListener(response -> {
                assertThat(response, equalTo(searchResponseMap));
                notifiedOfResultLatch.countDown();
            }));
            assertThat(queriedDatabaseLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            assertThat(notifiedOfResultLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            EnrichStatsAction.Response.CacheStats cacheStats = enrichCache.getStats(randomAlphaOfLength(10));
            assertThat(cacheStats.misses(), equalTo(++expectedMisses));
        }

        {
            // Do a computeIfAbsent with a different lookup value, assert that it is a cache miss and the search is performed:
            CountDownLatch queriedDatabaseLatch = new CountDownLatch(1);
            CountDownLatch notifiedOfResultLatch = new CountDownLatch(1);
            enrichCache.computeIfAbsent("policy1-1", projectId, "2", 1, (searchResponseActionListener) -> {
                SearchResponse searchResponse = convertToSearchResponse(searchResponseMap);
                searchResponseActionListener.onResponse(searchResponse);
                searchResponse.decRef();
                queriedDatabaseLatch.countDown();
            }, assertNoFailureListener(response -> {
                assertThat(response, equalTo(searchResponseMap));
                notifiedOfResultLatch.countDown();
            }));
            assertThat(queriedDatabaseLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            assertThat(notifiedOfResultLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            EnrichStatsAction.Response.CacheStats cacheStats = enrichCache.getStats(randomAlphaOfLength(10));
            assertThat(cacheStats.misses(), equalTo(++expectedMisses));
        }

        {
            // Do a computeIfAbsent with a different max matches, assert that it is a cache miss and the search is performed:
            CountDownLatch queriedDatabaseLatch = new CountDownLatch(1);
            CountDownLatch notifiedOfResultLatch = new CountDownLatch(1);
            enrichCache.computeIfAbsent("policy1-1", projectId, "1", 3, (searchResponseActionListener) -> {
                SearchResponse searchResponse = convertToSearchResponse(searchResponseMap);
                searchResponseActionListener.onResponse(searchResponse);
                searchResponse.decRef();
                queriedDatabaseLatch.countDown();
            }, assertNoFailureListener(response -> {
                assertThat(response, equalTo(searchResponseMap));
                notifiedOfResultLatch.countDown();
            }));
            assertThat(queriedDatabaseLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            assertThat(notifiedOfResultLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            EnrichStatsAction.Response.CacheStats cacheStats = enrichCache.getStats(randomAlphaOfLength(10));
            assertThat(cacheStats.misses(), equalTo(++expectedMisses));
        }
    }

    private SearchResponse convertToSearchResponse(List<Map<String, ?>> searchResponseList) {
        SearchHit[] hitArray = searchResponseList.stream().map(map -> {
            try {
                return SearchHit.unpooled(0, "id").sourceRef(convertMapToJson(map));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toArray(SearchHit[]::new);
        SearchHits hits = SearchHits.unpooled(hitArray, null, 0);
        return SearchResponseUtils.response(hits).shards(5, 4, 0).build();
    }

    private BytesReference convertMapToJson(Map<String, ?> simpleMap) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(simpleMap)) {
            return BytesReference.bytes(builder);
        }
    }

    public void testDeepCopy() {
        Map<String, Object> original = new HashMap<>();
        {
            original.put("foo", "bar");
            original.put("int", 123);
            original.put("double", 123.0D);
            Map<String, Object> innerObject = new HashMap<>();
            innerObject.put("buzz", "hello world");
            innerObject.put("foo_null", null);
            innerObject.put("1", "bar");
            innerObject.put("long", 123L);
            List<String> innerInnerList = new ArrayList<>();
            innerInnerList.add("item1");
            List<Object> innerList = new ArrayList<>();
            innerList.add(innerInnerList);
            innerObject.put("list", innerList);
            original.put("fizz", innerObject);
            List<Map<String, Object>> list = new ArrayList<>();
            Map<String, Object> value = new HashMap<>();
            value.put("field", "value");
            list.add(value);
            list.add(null);
            original.put("list", list);
            List<String> list2 = new ArrayList<>();
            list2.add("foo");
            list2.add("bar");
            list2.add("baz");
            original.put("list2", list2);
        }

        Map<?, ?> result = EnrichCache.deepCopy(original, false);
        assertThat(result, equalTo(original));
        assertThat(result, not(sameInstance(original)));

        result = EnrichCache.deepCopy(original, true);
        assertThat(result, equalTo(original));
        assertThat(result, not(sameInstance(original)));
        Map<?, ?> innerMap = (Map<?, ?>) result.get("fizz");
        expectThrows(UnsupportedOperationException.class, () -> innerMap.remove("x"));
        List<?> innerList = (List<?>) result.get("list");
        expectThrows(UnsupportedOperationException.class, () -> innerList.remove(0));

        original.put("embedded_object", new byte[] { 1, 2, 3 });
        result = EnrichCache.deepCopy(original, false);
        assertArrayEquals(new byte[] { 1, 2, 3 }, (byte[]) result.get("embedded_object"));
    }

}
