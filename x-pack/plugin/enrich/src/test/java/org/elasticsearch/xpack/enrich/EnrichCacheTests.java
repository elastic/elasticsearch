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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class EnrichCacheTests extends ESTestCase {

    private static final List<Map<String, ?>> RESPONSE = List.of(Map.of("key1", "value1", "key2", "value2"), Map.of("key3", "value3"));
    private static final Consumer<ActionListener<SearchResponse>> NO_SEARCH = listener -> fail(
        "Expected no call to the database because item should have been in the cache"
    );

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    // the relative time provider advances 1ms per call, so every operation appears to take 1ms
    private final AtomicLong testNanoTime = new AtomicLong(0);
    private final EnrichCache cache = new EnrichCache(
        100,
        threadContext,
        () -> testNanoTime.addAndGet(TimeValue.timeValueMillis(1).getNanos())
    );
    private final ProjectId project = randomProjectIdOrDefault();
    private final CapturingFetcher fetcher = new CapturingFetcher();

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

        EnrichCache enrichCache = new EnrichCache(3, threadContext);
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
        EnrichCache enrichCache = new EnrichCache(3, threadContext, () -> testNanoTime.addAndGet(TimeValue.timeValueMillis(1).getNanos()));

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
        assertThat(enrichCache.inFlightCount(), equalTo(0));
    }

    // concurrent misses for one key share one search, each receives its own copy, and a later lookup is a cache hit
    public void testConcurrentMissesAreCoalesced() {
        int numLookups = randomIntBetween(2, 32);
        List<List<Map<?, ?>>> results = new ArrayList<>();
        for (int i = 0; i < numLookups; i++) {
            lookup(assertNoFailureListener(results::add));
        }
        assertThat(fetcher.invocations.get(), equalTo(1));
        assertThat(results, empty());
        assertThat(cache.inFlightCount(), equalTo(1));
        assertThat(stats().misses(), equalTo((long) numLookups));

        fetcher.complete(RESPONSE);

        assertThat(results, hasSize(numLookups));
        Set<Object> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        for (List<Map<?, ?>> result : results) {
            assertThat(result, equalTo(RESPONSE));
            assertTrue("every lookup receives its own copy", distinct.add(result) && distinct.add(result.get(0)));
        }
        assertThat(stats().count(), equalTo(1L));
        assertThat(stats().hits(), equalTo(0L));
        // every follower's wait is accounted as miss time
        assertThat(stats().missesTimeInMillis(), greaterThan((long) numLookups));
        assertThat(cache.inFlightCount(), equalTo(0));

        // a late lookup is a cache hit
        lookup(NO_SEARCH, assertNoFailureListener(results::add));
        assertThat(results, hasSize(numLookups + 1));
        assertThat(results.get(numLookups), equalTo(RESPONSE));
        assertThat(stats().hits(), equalTo(1L));
        assertThat(stats().misses(), equalTo((long) numLookups));
        assertThat(cache.inFlightCount(), equalTo(0));
    }

    // #84781 analogue: a subscriber (leader or follower) whose continuation throws fails only its own lookup
    public void testThrowingSubscriberDoesNotAffectOthers() {
        int numLookups = randomIntBetween(2, 16);
        int throwingIndex = randomIntBetween(0, numLookups - 1);
        AtomicInteger completed = new AtomicInteger();
        AtomicReference<Exception> throwerFailure = new AtomicReference<>();
        for (int i = 0; i < numLookups; i++) {
            if (i == throwingIndex) {
                lookup(
                    ActionListener.wrap(
                        response -> { throw new IllegalArgumentException("field [users] not present as part of path [users]"); },
                        e -> assertTrue(throwerFailure.compareAndSet(null, e))
                    )
                );
            } else {
                lookup(assertNoFailureListener(response -> {
                    assertThat(response, equalTo(RESPONSE));
                    completed.incrementAndGet();
                }));
            }
        }
        assertThat(fetcher.invocations.get(), equalTo(1));

        // an exception escaping the deduplicator's fan-out would propagate out of here
        fetcher.complete(RESPONSE);

        assertThat(completed.get(), equalTo(numLookups - 1));
        assertThat(throwerFailure.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwerFailure.get().getMessage(), equalTo("field [users] not present as part of path [users]"));
        assertThat(stats().count(), equalTo(1L));
        assertThat(cache.inFlightCount(), equalTo(0));
    }

    // #82340 guard for the coalesced path: a subscriber mutating its result does not affect other subscribers or the cached value
    public void testSubscriberMutationsDoNotLeakIntoCacheOrOtherSubscribers() {
        final List<Map<String, ?>> response = List.of(
            Map.of("key1", "value1", "nested", Map.of("inner", "value2"), "list", List.of("a", "b"))
        );
        int numSubscribers = randomIntBetween(2, 16);
        AtomicInteger completed = new AtomicInteger();
        for (int i = 0; i < numSubscribers; i++) {
            lookup(assertNoFailureListener(result -> {
                assertThat(result, equalTo(response));
                mutateHit(result.get(0));
                completed.incrementAndGet();
            }));
        }
        assertThat(fetcher.invocations.get(), equalTo(1));

        fetcher.complete(response);
        assertThat(completed.get(), equalTo(numSubscribers));

        // a later lookup is served from the cache and must not see any of the mutations
        lookup(NO_SEARCH, assertNoFailureListener(result -> {
            assertThat(result, equalTo(response));
            completed.incrementAndGet();
        }));
        assertThat(completed.get(), equalTo(numSubscribers + 1));
        assertThat(stats().hits(), equalTo(1L));
        assertThat(cache.inFlightCount(), equalTo(0));
    }

    @SuppressWarnings("unchecked")
    private static void mutateHit(Map<?, ?> hit) {
        Map<String, Object> mutable = (Map<String, Object>) hit;
        mutable.put("key1", "mutated");
        ((Map<String, Object>) mutable.get("nested")).put("inner", "mutated");
        ((List<Object>) mutable.get("list")).clear();
    }

    // a failed search or an unparseable response fails every subscriber, caches nothing, and the next lookup searches again
    public void testAsyncLoadFailureFailsAllSubscribersAndIsNotCached() {
        int numLookups = randomIntBetween(2, 32);
        List<Exception> failures = new ArrayList<>();
        for (int i = 0; i < numLookups; i++) {
            lookup(ActionListener.wrap(response -> fail("expected a failure"), failures::add));
        }
        assertThat(fetcher.invocations.get(), equalTo(1));
        assertThat(cache.inFlightCount(), equalTo(1));

        if (randomBoolean()) {
            EsRejectedExecutionException rejection = new EsRejectedExecutionException("enrich coordination queue at capacity", false);
            fetcher.fail(rejection);
            assertThat(failures, everyItem(sameInstance(rejection)));
        } else {
            fetcher.complete(unparseableSearchResponse());
            assertThat(failures, everyItem(instanceOf(XContentParseException.class)));
        }
        assertThat(failures, hasSize(numLookups));
        assertThat(stats().count(), equalTo(0L));
        assertThat(cache.inFlightCount(), equalTo(0));

        // the failure was not cached: the next lookup runs the search again and succeeds
        AtomicReference<List<Map<?, ?>>> result = new AtomicReference<>();
        lookup(assertNoFailureListener(result::set));
        assertThat(fetcher.invocations.get(), equalTo(2));
        fetcher.complete(RESPONSE);
        assertThat(result.get(), equalTo(RESPONSE));
        assertThat(stats().count(), equalTo(1L));
        assertThat(cache.inFlightCount(), equalTo(0));
    }

    // a fetcher that fails synchronously (queue-full 429) or throws leaves no in-flight entry behind
    public void testSynchronousFetcherFailure() {
        boolean throwing = randomBoolean();
        AtomicInteger invocations = new AtomicInteger();
        EsRejectedExecutionException rejection = new EsRejectedExecutionException("enrich coordination queue at capacity", false);
        Consumer<ActionListener<SearchResponse>> failingFetcher = listener -> {
            invocations.incrementAndGet();
            if (throwing) {
                throw rejection;
            }
            listener.onFailure(rejection);
        };

        int numLookups = randomIntBetween(1, 8);
        for (int i = 0; i < numLookups; i++) {
            AtomicReference<Exception> failure = new AtomicReference<>();
            lookup(
                failingFetcher,
                ActionListener.wrap(response -> fail("expected a failure"), e -> assertTrue(failure.compareAndSet(null, e)))
            );
            assertThat(failure.get(), sameInstance(rejection));
            assertThat(invocations.get(), equalTo(i + 1));
            assertThat(cache.inFlightCount(), equalTo(0));
        }
        assertThat(stats().count(), equalTo(0L));
        assertThat(stats().misses(), equalTo((long) numLookups));
    }

    // parallel lookups over a few keys run exactly one search per key and complete every listener exactly once
    public void testConcurrentLookupsStress() throws Exception {
        int numKeys = randomIntBetween(1, 4);
        int numLookups = randomIntBetween(numKeys, 64);
        Map<String, Queue<ActionListener<SearchResponse>>> captured = new ConcurrentHashMap<>();
        AtomicInteger fetches = new AtomicInteger();
        AtomicInteger completions = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        CountDownLatch allSubscribed = new CountDownLatch(numLookups);

        // one task per lookup, plus a completer that finishes the captured searches once every lookup has subscribed, like a single
        // search response thread fanning out to coalesced lookups
        startInParallel(numLookups + 1, i -> {
            if (i == numLookups) {
                safeAwait(allSubscribed);
                for (Queue<ActionListener<SearchResponse>> listeners : captured.values()) {
                    ActionListener<SearchResponse> listener;
                    while ((listener = listeners.poll()) != null) {
                        SearchResponse response = convertToSearchResponse(RESPONSE);
                        listener.onResponse(response);
                        response.decRef();
                    }
                }
                return;
            }
            String lookupValue = "value-" + (i % numKeys);
            cache.computeIfAbsent(project, "policy1-1", lookupValue, 1, listener -> {
                fetches.incrementAndGet();
                captured.computeIfAbsent(lookupValue, k -> new ConcurrentLinkedQueue<>()).add(listener);
            }, ActionListener.wrap(response -> {
                assertThat(response, equalTo(RESPONSE));
                completions.incrementAndGet();
            }, e -> failures.incrementAndGet()));
            allSubscribed.countDown();
        });

        // startInParallel returns only once every task, the completer included, has finished
        assertThat(failures.get(), equalTo(0));
        assertThat(completions.get(), equalTo(numLookups));
        assertThat(fetches.get(), equalTo(numKeys));
        assertThat(stats().count(), equalTo((long) numKeys));
        assertThat(cache.inFlightCount(), equalTo(0));

        // second round: everything is a cache hit
        AtomicInteger hits = new AtomicInteger();
        startInParallel(numLookups, i -> {
            String lookupValue = "value-" + (i % numKeys);
            cache.computeIfAbsent(project, "policy1-1", lookupValue, 1, NO_SEARCH, assertNoFailureListener(response -> {
                assertThat(response, equalTo(RESPONSE));
                hits.incrementAndGet();
            }));
        });
        assertThat(hits.get(), equalTo(numLookups));
        assertThat(fetches.get(), equalTo(numKeys));
        assertThat(stats().hits(), equalTo((long) numLookups));
        assertThat(cache.inFlightCount(), equalTo(0));
    }

    // each subscriber is completed in the thread context it subscribed with, not in the completing thread's context
    public void testSubscriberCompletesInOwnThreadContext() {
        String header = "test-header";
        AtomicReference<String> leaderSaw = new AtomicReference<>();
        AtomicReference<String> followerSaw = new AtomicReference<>();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(header, "A");
            lookup(assertNoFailureListener(response -> assertTrue(leaderSaw.compareAndSet(null, threadContext.getHeader(header)))));
        }
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(header, "B");
            lookup(assertNoFailureListener(response -> assertTrue(followerSaw.compareAndSet(null, threadContext.getHeader(header)))));
        }
        assertThat(fetcher.invocations.get(), equalTo(1));

        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(header, "completer");
            fetcher.complete(RESPONSE);
            assertThat(threadContext.getHeader(header), equalTo("completer"));
        }
        assertThat(leaderSaw.get(), equalTo("A"));
        assertThat(followerSaw.get(), equalTo("B"));
        assertThat(cache.inFlightCount(), equalTo(0));
    }

    /**
     * Verifies that byte arrays in enrich source documents are independently deep-copied on each cache access. Uses SMILE-encoded
     * source documents so that binary fields survive the SearchHit#getSourceAsMap() round-trip as byte[], exercising the byte[]
     * branch in EnrichCache#deepCopy through the full computeIfAbsent → toCacheValue pipeline.
     */
    public void testByteArraySourceIsolation() throws InterruptedException {
        EnrichCache enrichCache = new EnrichCache(10, threadContext);
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

        // Mutating the first result must not corrupt the second, nor the cached value served afterwards
        resultBytes1[0] = 99;
        assertArrayEquals(originalBytes, resultBytes2);
        byte[] cachedBytes = (byte[]) enrichCache.get(new EnrichCache.CacheKey(projectId, "policy1-1", "1", 1)).get(0).get("binary_field");
        assertArrayEquals(originalBytes, cachedBytes);
        assertThat(enrichCache.inFlightCount(), equalTo(0));
    }

    private void lookup(ActionListener<List<Map<?, ?>>> listener) {
        lookup(fetcher, listener);
    }

    private void lookup(Consumer<ActionListener<SearchResponse>> searchResponseFetcher, ActionListener<List<Map<?, ?>>> listener) {
        cache.computeIfAbsent(project, "policy1-1", "1", 1, searchResponseFetcher, listener);
    }

    private EnrichStatsAction.Response.CacheStats stats() {
        return cache.getStats("_id");
    }

    private static SearchResponse unparseableSearchResponse() {
        SearchHit hit = new SearchHit(0, "id").sourceRef(new BytesArray("{ this is not json"));
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, null, 0);
        SearchResponse response = SearchResponseUtils.response(hits).shards(5, 4, 0).build();
        hits.decRef();
        return response;
    }

    // captures the listener of the single search it is asked to run, so that a test can complete or fail it later
    private static class CapturingFetcher implements Consumer<ActionListener<SearchResponse>> {
        final AtomicInteger invocations = new AtomicInteger();
        final AtomicReference<ActionListener<SearchResponse>> captured = new AtomicReference<>();

        @Override
        public void accept(ActionListener<SearchResponse> listener) {
            invocations.incrementAndGet();
            assertTrue("a previous search has not been completed yet", captured.compareAndSet(null, listener));
        }

        void complete(List<Map<String, ?>> hits) {
            complete(convertToSearchResponse(hits));
        }

        void complete(SearchResponse response) {
            ActionListener<SearchResponse> listener = captured.getAndSet(null);
            assertThat(listener, notNullValue());
            listener.onResponse(response);
            response.decRef();
        }

        void fail(Exception e) {
            ActionListener<SearchResponse> listener = captured.getAndSet(null);
            assertThat(listener, notNullValue());
            listener.onFailure(e);
        }
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

    private static SearchResponse convertToSearchResponse(List<Map<String, ?>> searchResponseList) {
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

    private static BytesReference convertMapToJson(Map<String, ?> simpleMap) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(simpleMap)) {
            return BytesReference.bytes(builder);
        }
    }

}
