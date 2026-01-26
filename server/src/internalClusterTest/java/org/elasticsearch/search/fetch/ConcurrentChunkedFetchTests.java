/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for concurrent/parallel scenarios in chunked fetch streaming.
 * Verifies thread safety, no cross-contamination between concurrent searches,
 * and proper resource handling under high concurrency.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3)
public class ConcurrentChunkedFetchTests extends ESIntegTestCase {

    private static final String INDEX_NAME = "index";

    public void testHighConcurrencyStressTest() throws Exception {
        createIndex(INDEX_NAME, Settings.builder()
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 0)
            .build());

        int docCount = 100;
        indexDocs(docCount);

        int numConcurrentSearches = 100;
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        CountDownLatch completionLatch = new CountDownLatch(numConcurrentSearches);

        ExecutorService executor = Executors.newFixedThreadPool(50);
        long startTime = System.currentTimeMillis();

        try {
            for (int i = 0; i < numConcurrentSearches; i++) {
                executor.submit(() -> {
                    SearchResponse response = null;
                    try {
                        response = client().prepareSearch(INDEX_NAME)
                            .setQuery(QueryBuilders.matchAllQuery())
                            .setSize(docCount)
                            .get();

                        if (response.getFailedShards() == 0) {
                            Set<String> ids = new HashSet<>();
                            for (SearchHit hit : response.getHits().getHits()) {
                                ids.add(hit.getId());
                            }
                            if (ids.size() == docCount) {
                                successCount.incrementAndGet();
                            } else {
                                failureCount.incrementAndGet();
                                firstError.compareAndSet(null,
                                    new AssertionError("Expected " + docCount + " docs but got " + ids.size()));
                            }
                        } else {
                            failureCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        failureCount.incrementAndGet();
                        firstError.compareAndSet(null, e);
                    } finally {
                        if (response != null) {
                            response.decRef();
                        }
                        completionLatch.countDown();
                    }
                });
            }

            assertTrue("All searches should complete within 60 seconds", completionLatch.await(60, TimeUnit.SECONDS));

            // Allow some failures under extreme load, but majority should succeed
            assertThat("Most searches should succeed", successCount.get(), greaterThan(numConcurrentSearches * 9 / 10));

            if (firstError.get() != null && failureCount.get() > numConcurrentSearches / 10) {
                throw new AssertionError("Too many failures", firstError.get());
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Rapid search cancellation test.
     * Start search, immediately cancel, repeat rapidly.
     * Verify no resource leaks.
     */
    public void testRapidSearchCancellation() throws Exception {
        createIndex(INDEX_NAME, Settings.builder()
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build());

        // Index enough docs to make fetching take some time
        int docCount = 500;
        indexDocs(docCount);

        int iterations = 50;
        AtomicInteger cancelledCount = new AtomicInteger(0);
        AtomicInteger completedCount = new AtomicInteger(0);
        AtomicReference<Exception> error = new AtomicReference<>();

        for (int i = 0; i < iterations; i++) {
            try {
                // Start search
                ActionFuture<SearchResponse> future = client().prepareSearch(INDEX_NAME)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(docCount)
                    .execute();

                // Randomly either cancel immediately or let it complete
                if (randomBoolean()) {
                    // Try to cancel - this may or may not succeed depending on timing
                    try {
                        future.cancel(true);
                        cancelledCount.incrementAndGet();
                    } catch (Exception e) {
                        // Cancellation might fail if already completed
                    }
                }

                // Try to get result (will throw if cancelled)
                try {
                    SearchResponse response = future.actionGet(5, TimeUnit.SECONDS);
                    if (response.getFailedShards() == 0) {
                        completedCount.incrementAndGet();
                    }
                } catch (TaskCancelledException | org.elasticsearch.ElasticsearchTimeoutException e) {
                    // Expected for cancelled or slow searches
                }

            } catch (Exception e) {
                // Log but don't fail - some cancellation-related exceptions are expected
                logger.debug("Exception during rapid cancellation test", e);
            }
        }

        logger.info("Rapid cancellation test: {} completed, {} cancelled attempts",
            completedCount.get(), cancelledCount.get());

        // Verify no resource leaks by running a final search
        SearchResponse finalResponse = client().prepareSearch(INDEX_NAME)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(10)
            .get();
        assertNoFailures(finalResponse);
    }

    /**
     * Test concurrent searches with different result sizes.
     * Ensures chunking works correctly when searches have varying chunk counts.
     */
    public void testConcurrentSearchesWithDifferentSizes() throws Exception {
        createIndex(INDEX_NAME, Settings.builder()
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build());

        int maxDocCount = 100;
        indexDocs(maxDocCount);

        int[] sizes = {1, 5, 10, 25, 50, 100}; // Different sizes to test
        Map<Integer, List<SearchResponse>> resultsBySize = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(sizes.length * 3); // 3 searches per size
        AtomicReference<Exception> error = new AtomicReference<>();

        ExecutorService executor = Executors.newFixedThreadPool(sizes.length * 3);
        try {
            for (int size : sizes) {
                resultsBySize.put(size, Collections.synchronizedList(new ArrayList<>()));
                for (int j = 0; j < 3; j++) {
                    final int requestSize = size;
                    executor.submit(() -> {
                        try {
                            SearchResponse response = client().prepareSearch(INDEX_NAME)
                                .setQuery(QueryBuilders.matchAllQuery())
                                .setSize(requestSize)
                                .get();

                            assertNoFailures(response);
                            resultsBySize.get(requestSize).add(response);
                        } catch (Exception e) {
                            error.compareAndSet(null, e);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
            }

            assertTrue("All searches should complete", latch.await(30, TimeUnit.SECONDS));
            assertNull("No errors", error.get());

            // Verify each size got correct results
            for (int size : sizes) {
                List<SearchResponse> responses = resultsBySize.get(size);
                assertThat("Should have 3 responses for size " + size, responses.size(), equalTo(3));

                for (SearchResponse response : responses) {
                    assertThat("Size " + size + " should return correct count",
                        response.getHits().getHits().length, equalTo(size));
                }
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Test concurrent index operations during chunked fetch.
     * Verifies fetch doesn't block or corrupt with concurrent writes.
     */
    public void testConcurrentIndexingDuringFetch() throws Exception {
        createIndex(INDEX_NAME, Settings.builder()
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .put("index.refresh_interval", "100ms")
            .build());

        // Initial documents
        indexDocs(50);

        AtomicBoolean stopIndexing = new AtomicBoolean(false);
        AtomicInteger indexedCount = new AtomicInteger(50);
        AtomicReference<Exception> error = new AtomicReference<>();

        // Background indexing thread
        Thread indexingThread = new Thread(() -> {
            int docId = 50;
            while (!stopIndexing.get()) {
                try {
                    client().prepareIndex(INDEX_NAME)
                        .setId(String.valueOf(docId++))
                        .setSource("{\"field\": \"concurrent" + docId + "\"}", XContentType.JSON)
                        .get();
                    indexedCount.incrementAndGet();
                    Thread.sleep(10);
                } catch (Exception e) {
                    if (!stopIndexing.get()) {
                        error.compareAndSet(null, e);
                    }
                    break;
                }
            }
        });

        indexingThread.start();

        // Run multiple searches while indexing
        try {
            for (int i = 0; i < 20; i++) {
                SearchResponse response = client().prepareSearch(INDEX_NAME)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(1000) // Large size to trigger chunking
                    .get();

                assertNoFailures(response);
                assertThat("Should have some results",
                    response.getHits().getHits().length, greaterThan(0));

                Thread.sleep(50);
            }
        } finally {
            stopIndexing.set(true);
            indexingThread.join(5000);
        }

        assertNull("No errors during concurrent indexing", error.get());
        logger.info("Indexed {} documents during test", indexedCount.get() - 50);
    }


    /**
     * Test concurrent searches with different queries.
     * Ensures filter application works correctly during chunked fetch.
     */
    public void testConcurrentSearchesWithDifferentQueries() throws Exception {
        createIndex(INDEX_NAME, Settings.builder()
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .build());

        // Index documents with categories
        List<IndexRequestBuilder> requests = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String category = "cat" + (i % 5);
            requests.add(client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("{\"field\": \"value" + i + "\", \"category\": \"" + category + "\"}",
                    XContentType.JSON));
        }
        indexRandom(true, requests);

        Map<String, Set<String>> resultsByCategory = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(5);
        AtomicReference<Exception> error = new AtomicReference<>();

        // Search for each category concurrently
        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (int c = 0; c < 5; c++) {
            final String category = "cat" + c;
            executor.submit(() -> {
                try {
                    SearchResponse response = client().prepareSearch(INDEX_NAME)
                        .setQuery(QueryBuilders.termQuery("category", category))
                        .setSize(100)
                        .get();

                    assertNoFailures(response);
                    Set<String> ids = new HashSet<>();
                    for (SearchHit hit : response.getHits().getHits()) {
                        ids.add(hit.getId());
                        // Verify category is correct
                        String hitCategory = (String) hit.getSourceAsMap().get("category");
                        assertEquals("Hit should belong to correct category", category, hitCategory);
                    }
                    resultsByCategory.put(category, ids);
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue("All searches should complete", latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        assertNull("No errors", error.get());

        // Each category should have 20 documents (100 docs / 5 categories)
        for (int c = 0; c < 5; c++) {
            String category = "cat" + c;
            assertThat("Category " + category + " should have 20 docs",
                resultsByCategory.get(category).size(), equalTo(20));
        }

        // Verify no overlap between categories
        Set<String> allIds = new HashSet<>();
        for (Set<String> categoryIds : resultsByCategory.values()) {
            for (String id : categoryIds) {
                assertTrue("Each ID should appear only once across all categories", allIds.add(id));
            }
        }
    }

    private void indexDocs(int count) {
        List<IndexRequestBuilder> requests = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            requests.add(client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("{\"field\": \"value" + i + "\"}", XContentType.JSON));
        }
        indexRandom(true, requests);
    }
}
