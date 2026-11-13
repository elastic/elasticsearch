/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
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
import static org.elasticsearch.common.util.CollectionUtils.DeepCopyOption.UNMODIFIABLE;
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
        var cacheKey1 = new EnrichCache.CacheKey(projectId, "policy1-1", "1", 1);
        var cacheKey2 = new EnrichCache.CacheKey(projectId, "policy1-1", "2", 1);
        var cacheKey3 = new EnrichCache.CacheKey(projectId, "policy2-1", "1", 1);
        var cacheKey4 = new EnrichCache.CacheKey(projectId, "policy2-1", "2", 1);
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

        cacheKey1 = new EnrichCache.CacheKey(projectId, "policy1-2", "1", 1);
        cacheKey2 = new EnrichCache.CacheKey(projectId, "policy1-2", "2", 1);
        cacheKey3 = new EnrichCache.CacheKey(projectId, "policy2-2", "1", 1);
        cacheKey4 = new EnrichCache.CacheKey(projectId, "policy2-2", "2", 1);

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
            enrichCache.computeIfAbsent(projectId, "policy1-1", "1", 1, (searchResponseActionListener) -> {
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
            enrichCache.computeIfAbsent(projectId, "policy1-1", "1", 1, (searchResponseActionListener) -> {
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
            enrichCache.computeIfAbsent(projectId, "policy1-2", "1", 1, (searchResponseActionListener) -> {
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
            enrichCache.computeIfAbsent(randomUniqueProjectId(), "policy1-1", "1", 1, (searchResponseActionListener) -> {
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
            enrichCache.computeIfAbsent(projectId, "policy1-1", "2", 1, (searchResponseActionListener) -> {
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
            enrichCache.computeIfAbsent(projectId, "policy1-1", "1", 3, (searchResponseActionListener) -> {
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

    /**
     * Verifies that byte arrays in enrich source documents are independently deep-copied on each cache access. Uses SMILE-encoded
     * source documents so that binary fields survive the SearchHit#getSourceAsMap() round-trip as byte[], exercising the byte[]
     * branch in EnrichCache#deepCopy through the full computeIfAbsent → toCacheValue pipeline.
     */
    public void testByteArraySourceIsolation() throws InterruptedException {
        EnrichCache enrichCache = new EnrichCache(10);
        ProjectId projectId = randomProjectIdOrDefault();

        final byte[] originalBytes = { 1, 2, 3, 4, 5 };
        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("key1", "value1");
        sourceMap.put("binary_field", originalBytes);

        List<List<Map<?, ?>>> capturedResults = new ArrayList<>();

        // First call: cache miss — source document is fetched and cached
        {
            CountDownLatch queriedDatabaseLatch = new CountDownLatch(1);
            CountDownLatch notifiedOfResultLatch = new CountDownLatch(1);
            enrichCache.computeIfAbsent(projectId, "policy1-1", "1", 1, listener -> {
                SearchResponse searchResponse = convertToSearchResponseWithSmile(sourceMap);
                listener.onResponse(searchResponse);
                searchResponse.decRef();
                queriedDatabaseLatch.countDown();
            }, assertNoFailureListener(response -> {
                capturedResults.add(response);
                notifiedOfResultLatch.countDown();
            }));
            assertThat(queriedDatabaseLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            assertThat(notifiedOfResultLatch.await(5, TimeUnit.SECONDS), equalTo(true));
        }

        // Second call: cache hit — result must be a fresh deep copy, not shared with the first
        {
            CountDownLatch notifiedOfResultLatch = new CountDownLatch(1);
            enrichCache.computeIfAbsent(projectId, "policy1-1", "1", 1, listener -> {
                fail("Expected no call to the database because item should have been in the cache");
            }, assertNoFailureListener(response -> {
                capturedResults.add(response);
                notifiedOfResultLatch.countDown();
            }));
            assertThat(notifiedOfResultLatch.await(5, TimeUnit.SECONDS), equalTo(true));
        }

        assertThat(capturedResults.size(), equalTo(2));
        byte[] resultBytes1 = (byte[]) capturedResults.get(0).get(0).get("binary_field");
        byte[] resultBytes2 = (byte[]) capturedResults.get(1).get(0).get("binary_field");

        // Both results must have the correct values
        assertArrayEquals(originalBytes, resultBytes1);
        assertArrayEquals(originalBytes, resultBytes2);

        // Each call returns a distinct copy; no result shares an instance with the original or each other
        assertThat(resultBytes1, not(sameInstance(originalBytes)));
        assertThat(resultBytes2, not(sameInstance(originalBytes)));
        assertThat(resultBytes1, not(sameInstance(resultBytes2)));

        // Mutating the first result must not corrupt the second
        resultBytes1[0] = 99;
        assertArrayEquals(originalBytes, resultBytes2);
    }

    // Expected sizes assume compressed oops and 8-byte alignment (header 12, array header 16, reference 4): map 80 + table + 32 per
    // entry with keys free, list 48 + array, Double/Long 24, Integer/Float/Boolean 16, String 24 + align(16 + 2 * length).
    private static void assumeCompressedOops() {
        assumeTrue(
            "expected sizes are hand-computed for compressed oops and 8-byte alignment",
            RamUsageEstimator.COMPRESSED_REFS_ENABLED && RamUsageEstimator.NUM_BYTES_OBJECT_ALIGNMENT == 8
        );
    }

    public void testEstimateHeapSizeOfFlatNumericMap() {
        assumeCompressedOops();
        assertThat(EnrichCache.estimateHeapSize(null), equalTo(0L));
        assertThat(EnrichCache.estimateHeapSize(Map.of()), equalTo(80L));
        // 3 entries: ceil(3 / 0.75) = 4 -> table align(16 + 16) = 32; nodes 3 * 32 = 96; Double 24 + Long 24 + Integer 16 = 64
        Map<String, Object> flat = Map.of("d", 1.5d, "l", 7L, "i", 3);
        assertThat(EnrichCache.estimateHeapSize(flat), equalTo(80L + 32 + 96 + 64));
        // the estimate is of the deep copy's shape, so it is the same for the copy itself
        assertThat(EnrichCache.estimateHeapSize(CollectionUtils.deepCopy(flat, UNMODIFIABLE)), equalTo(272L));
    }

    public void testEstimateHeapSizeOfNestedMaps() {
        assumeCompressedOops();
        // outer, 1 entry: ceil(1 / 0.75) = 2 -> table align(16 + 8) = 24; one node 32 -> 80 + 24 + 32 = 136
        // inner, 2 entries: ceil(2 / 0.75) = 3 -> capacity 4 -> table 32; nodes 64; Integer 16 + Boolean 16 -> 80 + 32 + 64 + 32 = 208
        Map<String, Object> nested = Map.of("outer", Map.of("x", 1, "y", true));
        assertThat(EnrichCache.estimateHeapSize(nested), equalTo(136L + 208));
    }

    public void testEstimateHeapSizeOfLists() {
        assumeCompressedOops();
        // single-entry map 136; list of 3 Integers: 48 + array align(16 + 12) = 32 + 3 * 16 = 128
        assertThat(EnrichCache.estimateHeapSize(Map.of("list", List.of(1, 2, 3))), equalTo(136L + 128));
        // an empty list shares a static empty array, so only the shells are charged
        assertThat(EnrichCache.estimateHeapSize(Map.of("empty", List.of())), equalTo(136L + 48));
        // list of two single-entry maps: 48 + array align(16 + 8) = 24 + 2 * (136 + Long 24)
        assertThat(EnrichCache.estimateHeapSize(List.of(Map.of("a", 1L), Map.of("b", 2L))), equalTo(48L + 24 + 2 * 160));
    }

    public void testEstimateHeapSizeOfStrings() {
        assumeCompressedOops();
        // 20 characters: 24 + align(16 + 2 * 20) = 24 + 56 = 80, in a single-entry map 136
        assertThat(EnrichCache.estimateHeapSize(Map.of("s", "0123456789abcdefghij")), equalTo(136L + 80));
        // 200 characters: 24 + align(16 + 400) = 440
        assertThat(EnrichCache.estimateHeapSize(Map.of("s", "x".repeat(200))), equalTo(136L + 440));
        // strings are counted as UTF-16, so text the JVM cannot store as Latin-1 is not undercounted
        assertThat(EnrichCache.estimateHeapSize("東京都渋谷区"), equalTo(EnrichCache.estimateHeapSize("tokyos")));
        assertThat(EnrichCache.estimateHeapSize("tokyos"), equalTo(24L + 32));
        assertThat(EnrichCache.estimateHeapSize("tokyos"), equalTo(RamUsageEstimator.sizeOf("tokyos")));
    }

    public void testEstimateHeapSizeOfRareLeafTypes() {
        assumeCompressedOops();
        // byte[5]: align(16 + 5) = 24, in a single-entry map 136
        assertThat(EnrichCache.estimateHeapSize(Map.of("b", new byte[] { 1, 2, 3, 4, 5 })), equalTo(136L + 24));
        assertThat(EnrichCache.estimateHeapSize(new byte[1024]), equalTo(RamUsageEstimator.sizeOf(new byte[1024])));
        // Float, as parsed from SMILE or CBOR sources: 16, in a single-entry map 136
        assertThat(EnrichCache.estimateHeapSize(Map.of("f", 1.5f)), equalTo(136L + 16));
        // anything else is left to Lucene, which charges its default for types it does not know
        assertThat(EnrichCache.estimateHeapSize(new java.util.Date()), equalTo((long) RamUsageEstimator.UNKNOWN_DEFAULT_RAM_BYTES_USED));
    }

    /**
     * Verifies that the weight of a cache value is the cache key constant plus the estimated heap of the parsed hits rather than the
     * serialized size of the search response.
     */
    public void testToCacheValueWeighsEstimatedHeap() {
        assumeCompressedOops();
        SearchResponse emptyResponse = convertToSearchResponse(List.of());
        try {
            assertThat(EnrichCache.toCacheValue(emptyResponse).sizeInBytes(), equalTo(256L));
        } finally {
            emptyResponse.decRef();
        }
        // hits list: 48 + array align(16 + 8) = 24 -> 72
        // {"a": 1.5}: single-entry map 136 + Double 24 = 160
        // {"b": "xyz"}: single-entry map 136 + string 24 + align(16 + 6) = 48 -> 184
        SearchResponse response = convertToSearchResponse(List.of(Map.of("a", 1.5d), Map.of("b", "xyz")));
        try {
            EnrichCache.CacheValue cacheValue = EnrichCache.toCacheValue(response);
            assertThat(cacheValue.hits().size(), equalTo(2));
            assertThat(cacheValue.sizeInBytes(), equalTo(256L + 72 + 160 + 184));
        } finally {
            response.decRef();
        }
    }

    /**
     * Verifies that a byte-sized cache evicts based on the estimated heap of its entries and reports it as its size in bytes.
     */
    public void testEvictionByEstimatedHeapSize() {
        assumeCompressedOops();
        // one entry: key 256 + single-hit list 48 + align(16 + 4) = 24 + {"a": 1.5} 160 -> 488
        long entrySize = 488L;
        EnrichCache enrichCache = new EnrichCache(ByteSizeValue.ofBytes(2 * entrySize + 100));
        ProjectId projectId = randomProjectIdOrDefault();
        for (int i = 0; i < 3; i++) {
            SearchResponse response = convertToSearchResponse(List.of(Map.of("a", 1.5d)));
            try {
                EnrichCache.CacheValue cacheValue = EnrichCache.toCacheValue(response);
                assertThat(cacheValue.sizeInBytes(), equalTo(entrySize));
                enrichCache.put(new EnrichCache.CacheKey(projectId, "policy1-1", String.valueOf(i), 1), cacheValue);
            } finally {
                response.decRef();
            }
        }
        EnrichStatsAction.Response.CacheStats cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.count(), equalTo(2L));
        assertThat(cacheStats.evictions(), equalTo(1L));
        assertThat(cacheStats.cacheSizeInBytes(), equalTo(2 * entrySize));
        // the oldest entry was evicted, the two most recent ones are still served
        assertThat(enrichCache.get(new EnrichCache.CacheKey(projectId, "policy1-1", "0", 1)), nullValue());
        assertThat(enrichCache.get(new EnrichCache.CacheKey(projectId, "policy1-1", "1", 1)), notNullValue());
        assertThat(enrichCache.get(new EnrichCache.CacheKey(projectId, "policy1-1", "2", 1)), notNullValue());
    }

    private SearchResponse convertToSearchResponseWithSmile(Map<String, Object> sourceMap) {
        try {
            SearchHit hit = new SearchHit(0, "id").sourceRef(convertMapToSmile(sourceMap));
            SearchHits hits = new SearchHits(new SearchHit[] { hit }, null, 0);
            SearchResponse response = SearchResponseUtils.response(hits).shards(5, 4, 0).build();
            hits.decRef();
            return response;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BytesReference convertMapToSmile(Map<String, ?> simpleMap) throws IOException {
        try (XContentBuilder builder = SmileXContent.contentBuilder().map(simpleMap)) {
            return BytesReference.bytes(builder);
        }
    }

    private SearchResponse convertToSearchResponse(List<Map<String, ?>> searchResponseList) {
        SearchHit[] hitArray = searchResponseList.stream().map(map -> {
            try {
                return new SearchHit(0, "id").sourceRef(convertMapToJson(map));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toArray(SearchHit[]::new);
        SearchHits hits = new SearchHits(hitArray, null, 0);
        SearchResponse response = SearchResponseUtils.response(hits).shards(5, 4, 0).build();
        hits.decRef(); // transfer ownership to response
        return response;
    }

    private BytesReference convertMapToJson(Map<String, ?> simpleMap) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(simpleMap)) {
            return BytesReference.bytes(builder);
        }
    }

}
