/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration tests for circuit breaker behavior when memory limits are exceeded
 * during chunked fetch operations.
 *
 * Tests verify that the circuit breaker properly trips when the coordinator
 * accumulates too much data, and that memory is correctly released even after
 * breaker failures. Uses a low 5MB limit to reliably trigger breaker trips with
 * large documents.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ChunkedFetchPhaseCircuitBreakerTrippingIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "idx";
    private static final String SORT_FIELD = "sort_field";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("indices.breaker.request.type", "memory")
            .put("indices.breaker.request.limit", "5mb") // Low limit to trigger breaker - 5MB
            .put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), true)
            .build();
    }

    public void testCircuitBreakerTripsOnCoordinator() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        createIndex(INDEX_NAME);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            builders.add(
                prepareIndex(INDEX_NAME).setId(Integer.toString(i))
                    .setSource(
                        jsonBuilder().startObject()
                            .field(SORT_FIELD, i)
                            .field("text", "document " + i)
                            .field("huge_content", Strings.repeat("x", 2_000_000))  // 2MB each
                            .endObject()
                    )
            );
        }
        indexRandom(true, builders);
        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);

        ElasticsearchException exception = null;
        SearchResponse resp = null;
        try {
            resp = internalCluster().client(coordinatorNode)
                .prepareSearch(INDEX_NAME)
                .setQuery(matchAllQuery())
                .setSize(5)  // Request 3 huge docs = ~6MB > 5MB limit
                .setAllowPartialSearchResults(false)
                .addSort(SORT_FIELD, SortOrder.ASC)
                .get();
        } catch (ElasticsearchException e) {
            exception = e;
        } finally {
            if (resp != null) {
                resp.decRef();
            }
        }

        Throwable cause = exception.getCause();
        while (cause != null && (cause instanceof CircuitBreakingException) == false) {
            cause = cause.getCause();
        }
        assertThat("Should have CircuitBreakingException in cause chain", cause, instanceOf(CircuitBreakingException.class));

        CircuitBreakingException breakerException = (CircuitBreakingException) cause;
        assertThat(breakerException.getMessage(), containsString("[request] Data too large"));

        assertThat(
            "Circuit breaking should map to 429 TOO_MANY_REQUESTS",
            ExceptionsHelper.status(exception),
            equalTo(RestStatus.TOO_MANY_REQUESTS)
        );

        assertBusy(() -> {
            long currentBreaker = getRequestBreakerUsed(coordinatorNode);
            assertThat(
                "Coordinator circuit breaker should be released even after tripping, current: "
                    + currentBreaker
                    + ", before: "
                    + breakerBefore,
                currentBreaker,
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    public void testCircuitBreakerTripsWithConcurrentSearches() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        createIndex(INDEX_NAME);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            builders.add(
                prepareIndex(INDEX_NAME).setId(Integer.toString(i))
                    .setSource(
                        jsonBuilder().startObject()
                            .field(SORT_FIELD, i)
                            .field("text", "document " + i)
                            .field("large_content", Strings.repeat("x", 1_500_000))  // 1.5MB each
                            .endObject()
                    )
            );
        }
        indexRandom(true, builders);
        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);

        int numSearches = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numSearches);
        try {
            List<CompletableFuture<Void>> futures = IntStream.range(0, numSearches).mapToObj(i -> CompletableFuture.runAsync(() -> {
                var client = internalCluster().client(coordinatorNode);
                var resp = client.prepareSearch(INDEX_NAME)
                    .setQuery(matchAllQuery())
                    .setSize(4)
                    .setAllowPartialSearchResults(false)
                    .addSort(SORT_FIELD, SortOrder.ASC)
                    .get();
                resp.decRef();
            }, executor)).toList();

            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).exceptionally(ex -> null).get(30, TimeUnit.SECONDS);

            List<Exception> exceptions = new ArrayList<>();
            for (CompletableFuture<Void> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    exceptions.add((Exception) e.getCause());
                }
            }
            assertThat("Expected at least one circuit breaker exception", exceptions.size(), greaterThan(0));

            boolean foundBreakerException = false;
            for (Exception e : exceptions) {
                if (containsCircuitBreakerException(e)) {
                    foundBreakerException = true;
                    break;
                }

                assertThat(
                    "Circuit breaking should map to 429 TOO_MANY_REQUESTS",
                    ExceptionsHelper.status(e),
                    equalTo(RestStatus.TOO_MANY_REQUESTS)
                );
            }
            assertThat("Should have found a CircuitBreakingException", foundBreakerException, equalTo(true));
        } finally {
            executor.shutdown();
            assertTrue("Executor should terminate", executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertBusy(() -> {
            long currentBreaker = getRequestBreakerUsed(coordinatorNode);
            assertThat(
                "Coordinator circuit breaker should recover after concurrent breaker trips, current: "
                    + currentBreaker
                    + ", before: "
                    + breakerBefore,
                currentBreaker,
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    public void testCircuitBreakerTripsOnSingleLargeDocument() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        createIndex(INDEX_NAME);

        prepareIndex(INDEX_NAME).setId("huge")
            .setSource(
                jsonBuilder().startObject()
                    .field(SORT_FIELD, 0)
                    .field("text", "huge document")
                    .field("huge_field", Strings.repeat("x", 6_000_000))  // 6MB
                    .endObject()
            )
            .get();
        populateLargeDocuments(INDEX_NAME, 10, 1_000);
        refresh(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);
        ElasticsearchException exception = null;
        SearchResponse resp = null;
        try {
            resp = internalCluster().client(coordinatorNode)
                .prepareSearch(INDEX_NAME)
                .setQuery(matchAllQuery())
                .setSize(5)
                .setAllowPartialSearchResults(false)
                .addSort(SORT_FIELD, SortOrder.ASC)
                .get();
        } catch (ElasticsearchException e) {
            exception = e;
        } finally {
            if (resp != null) {
                resp.decRef();
            }
        }

        boolean foundBreakerException = containsCircuitBreakerException(exception);
        assertThat("Circuit breaker should have tripped on single large document", foundBreakerException, equalTo(true));

        assertThat(
            "Circuit breaking should map to 429 TOO_MANY_REQUESTS",
            ExceptionsHelper.status(exception),
            equalTo(RestStatus.TOO_MANY_REQUESTS)
        );

        assertBusy(() -> {
            long currentBreaker = getRequestBreakerUsed(coordinatorNode);
            assertThat(
                "Coordinator circuit breaker should be released after single large doc trip",
                currentBreaker,
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    /**
     * Test that multiple sequential breaker trips don't cause memory leaks.
     * Repeatedly tripping the breaker should not accumulate memory.
     */
    public void testRepeatedCircuitBreakerTripsNoLeak() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        createIndex(INDEX_NAME);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            builders.add(
                prepareIndex(INDEX_NAME).setId(Integer.toString(i))
                    .setSource(
                        jsonBuilder().startObject()
                            .field(SORT_FIELD, i)
                            .field("text", "document " + i)
                            .field("large_content", Strings.repeat("x", 1_200_000))  // 1.2MB each
                            .endObject()
                    )
            );
        }
        indexRandom(true, builders);
        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        long initialBreaker = getRequestBreakerUsed(coordinatorNode);

        ElasticsearchException exception = null;
        for (int i = 0; i < 10; i++) {
            SearchResponse resp = null;
            try {
                resp = internalCluster().client(coordinatorNode)
                    .prepareSearch(INDEX_NAME)
                    .setQuery(matchAllQuery())
                    .setSize(5)  // 5 docs × 1.2MB = 6MB > 5MB limit
                    .setAllowPartialSearchResults(false)
                    .addSort(SORT_FIELD, SortOrder.ASC)
                    .get();
            } catch (ElasticsearchException e) {
                exception = e;
            } finally {
                if (resp != null) {
                    resp.decRef();
                }
            }
            Thread.sleep(100);
        }

        boolean foundBreakerException = containsCircuitBreakerException(exception);
        assertThat("Circuit breaker should have tripped on single large document", foundBreakerException, equalTo(true));

        assertThat(
            "Circuit breaking should map to 429 TOO_MANY_REQUESTS",
            ExceptionsHelper.status(exception),
            equalTo(RestStatus.TOO_MANY_REQUESTS)
        );

        assertBusy(() -> {
            long currentBreaker = getRequestBreakerUsed(coordinatorNode);
            assertThat(
                "Circuit breaker should not leak after repeated trips, current: " + currentBreaker + ", initial: " + initialBreaker,
                currentBreaker,
                lessThanOrEqualTo(initialBreaker)
            );
        });
    }

    private void populateLargeDocuments(String indexName, int nDocs, int contentSize) throws IOException {
        int batchSize = 10;
        for (int batch = 0; batch < nDocs; batch += batchSize) {
            int endDoc = Math.min(batch + batchSize, nDocs);
            List<IndexRequestBuilder> builders = new ArrayList<>();

            for (int i = batch; i < endDoc; i++) {
                builders.add(
                    prepareIndex(indexName).setId(Integer.toString(i))
                        .setSource(
                            jsonBuilder().startObject()
                                .field(SORT_FIELD, i)
                                .field("text", "document " + i)
                                .field("large_content", Strings.repeat("x", contentSize))
                                .endObject()
                        )
                );
            }
            indexRandom(batch == 0, builders);
        }
        refresh(indexName);
    }

    private void createIndex(String indexName) {
        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
                .setMapping(
                    SORT_FIELD,
                    "type=long",
                    "text",
                    "type=text,store=false",
                    "large_content",
                    "type=text,store=false",
                    "huge_field",
                    "type=text,store=false"
                )
        );
    }

    private long getRequestBreakerUsed(String nodeName) {
        CircuitBreakerService breakerService = internalCluster().getInstance(CircuitBreakerService.class, nodeName);
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        return breaker.getUsed();
    }

    private boolean containsCircuitBreakerException(Throwable t) {
        if (t == null) {
            return false;
        }
        if (t instanceof CircuitBreakingException) {
            return true;
        }
        if (t.getMessage() != null && t.getMessage().contains("CircuitBreakingException")) {
            return true;
        }
        return containsCircuitBreakerException(t.getCause());
    }
}
