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
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for chunked fetch phase circuit breaker tracking. The tests verify that the coordinator node properly
 * tracks and releases circuit breaker memory when using chunked fetch across multiple shards and nodes.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ChunkedFetchPhaseCircuitBreakerIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "chunked_multi_shard_idx";
    private static final String SORT_FIELD = "sort_field";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("indices.breaker.request.type", "memory")
            .put("indices.breaker.request.limit", "200mb")
            .put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), true)
            .build();
    }

    public void testChunkedFetchMultipleShardsSingleNode() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        createIndexForTest(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        populateIndex(INDEX_NAME, 150, 5_000);
        ensureGreen(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);

        assertNoFailuresAndResponse(
            internalCluster().client(coordinatorNode)
                .prepareSearch(INDEX_NAME)
                .setQuery(matchAllQuery())
                .setSize(100)
                .addSort(SORT_FIELD, SortOrder.ASC),
            response -> {
                assertThat(response.getHits().getHits().length, equalTo(100));
                verifyHitsOrder(response);
            }
        );

        assertBusy(() -> {
            assertThat(
                "Coordinator circuit breaker should be released after chunked fetch completes",
                getRequestBreakerUsed(coordinatorNode),
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    public void testChunkedFetchMultipleShardsMultipleNodes() throws Exception {
        internalCluster().startNode();
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        int numberOfShards = randomIntBetween(6, 24);
        createIndexForTest(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );

        int numberOfDocuments = randomIntBetween(300, 1000);
        populateIndex(INDEX_NAME, numberOfDocuments, 5_000);
        ensureGreen(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);
        assertNoFailuresAndResponse(
            internalCluster().client(coordinatorNode)
                .prepareSearch(INDEX_NAME)
                .setQuery(matchAllQuery())
                .setSize(300)
                .addSort(SORT_FIELD, SortOrder.ASC),
            response -> {
                assertThat(response.getHits().getHits().length, equalTo(300));
                verifyHitsOrder(response);
            }
        );

        assertBusy(() -> {
            long currentBreaker = getRequestBreakerUsed(coordinatorNode);
            assertThat(
                "Coordinator circuit breaker should be released after many-shard chunked fetch, current: "
                    + currentBreaker
                    + ", before: "
                    + breakerBefore,
                currentBreaker,
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    public void testChunkedFetchConcurrentSearches() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        createIndexForTest(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        populateIndex(INDEX_NAME, 110, 1_000);
        ensureGreen(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);

        int numSearches = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numSearches);
        try {
            List<CompletableFuture<Void>> futures = IntStream.range(0, numSearches).mapToObj(i -> CompletableFuture.runAsync(() -> {
                assertNoFailuresAndResponse(
                    internalCluster().client(coordinatorNode)
                        .prepareSearch(INDEX_NAME)
                        .setQuery(matchAllQuery())
                        .setSize(30)
                        .addSort(SORT_FIELD, SortOrder.ASC),
                    response -> assertThat(response.getHits().getHits().length, equalTo(30))
                );
            }, executor)).toList();

            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get(30, TimeUnit.SECONDS);
            assertThat("All concurrent searches should succeed", futures.size(), equalTo(numSearches));
        } finally {
            executor.shutdown();
            assertTrue("Executor should terminate", executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertBusy(() -> {
            long currentBreaker = getRequestBreakerUsed(coordinatorNode);
            assertThat(
                "Coordinator circuit breaker should be released after concurrent searches, current: "
                    + currentBreaker
                    + ", before: "
                    + breakerBefore,
                currentBreaker,
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    public void testChunkedFetchWithReplicas() throws Exception {
        internalCluster().startNode();
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        createIndexForTest(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );

        populateIndex(INDEX_NAME, 150, 3_000);
        ensureGreen(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);

        // Search will naturally hit both primaries and replicas due to load balancing
        assertNoFailuresAndResponse(
            internalCluster().client(coordinatorNode)
                .prepareSearch(INDEX_NAME)
                .setQuery(matchAllQuery())
                .setSize(100)
                .addSort(SORT_FIELD, SortOrder.ASC),
            response -> {
                assertThat(response.getHits().getHits().length, equalTo(100));
                verifyHitsOrder(response);
            }
        );

        assertBusy(() -> {
            long currentBreaker = getRequestBreakerUsed(coordinatorNode);
            assertThat(
                "Coordinator circuit breaker should be released after chunked fetch with replicas",
                currentBreaker,
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    public void testChunkedFetchWithFiltering() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        createIndexForTest(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        populateIndex(INDEX_NAME, 300, 2_000);
        ensureGreen(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);

        assertNoFailuresAndResponse(
            internalCluster().client(coordinatorNode)
                .prepareSearch(INDEX_NAME)
                .setQuery(termQuery("keyword", "value1"))
                .setSize(50)
                .addSort(SORT_FIELD, SortOrder.ASC),
            response -> {
                assertThat(response.getHits().getHits().length, greaterThan(0));
                // Verify all results match filter
                for (int i = 0; i < response.getHits().getHits().length; i++) {
                    assertThat(Objects.requireNonNull(response.getHits().getHits()[i].getSourceAsMap()).get("keyword"), equalTo("value1"));
                }
                verifyHitsOrder(response);
            }
        );

        assertBusy(() -> {
            assertThat(
                "Coordinator circuit breaker should be released after chunked fetch completes",
                getRequestBreakerUsed(coordinatorNode),
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    public void testChunkedFetchNoMemoryLeakSequential() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        createIndexForTest(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        populateIndex(INDEX_NAME, 200, 2_000);
        ensureGreen(INDEX_NAME);

        long initialBreaker = getRequestBreakerUsed(coordinatorNode);

        for (int i = 0; i < 50; i++) {
            assertNoFailuresAndResponse(
                internalCluster().client(coordinatorNode)
                    .prepareSearch(INDEX_NAME)
                    .setQuery(matchAllQuery())
                    .setSize(40)
                    .addSort(SORT_FIELD, SortOrder.ASC),
                response -> {
                    assertThat(response.getHits().getHits().length, equalTo(40));
                }
            );
        }

        assertBusy(() -> {
            long currentBreaker = getRequestBreakerUsed(coordinatorNode);
            assertThat(
                "Coordinator circuit breaker should not leak memory across sequential chunked fetches, current: "
                    + currentBreaker
                    + ", initial: "
                    + initialBreaker,
                currentBreaker,
                lessThanOrEqualTo(initialBreaker)
            );
        });
    }

    public void testChunkedFetchWithAggregations() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        createIndexForTest(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        populateIndex(INDEX_NAME, 250, 2_000);
        ensureGreen(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);

        assertNoFailuresAndResponse(
            internalCluster().client(coordinatorNode)
                .prepareSearch(INDEX_NAME)
                .setQuery(matchAllQuery())
                .setSize(100)
                .addAggregation(terms("keywords").field("keyword").size(10))
                .addSort(SORT_FIELD, SortOrder.ASC),
            response -> {
                assertThat(response.getHits().getHits().length, equalTo(100));
                verifyHitsOrder(response);

                // Verify aggregation results
                Terms keywordAgg = response.getAggregations().get("keywords");
                assertThat(keywordAgg, notNullValue());
                assertThat(keywordAgg.getBuckets().size(), equalTo(10));
            }
        );

        assertBusy(() -> {
            long currentBreaker = getRequestBreakerUsed(coordinatorNode);
            assertThat(
                "Coordinator circuit breaker should be released after chunked fetch with aggregations",
                currentBreaker,
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    public void testChunkedFetchWithSearchAfter() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        createIndexForTest(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        populateIndex(INDEX_NAME, 150, 2_000);
        ensureGreen(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);

        // First page
        SearchResponse response1 = internalCluster().client(coordinatorNode)
            .prepareSearch(INDEX_NAME)
            .setQuery(matchAllQuery())
            .setSize(30)
            .addSort(SORT_FIELD, SortOrder.ASC)
            .get();

        try {
            assertThat(response1.getHits().getHits().length, equalTo(30));
            Object[] lastSort = response1.getHits().getHits()[29].getSortValues();

            // Second page with search_after using same coordinator
            assertNoFailuresAndResponse(
                internalCluster().client(coordinatorNode)
                    .prepareSearch(INDEX_NAME)
                    .setQuery(matchAllQuery())
                    .setSize(30)
                    .addSort(SORT_FIELD, SortOrder.ASC)
                    .searchAfter(lastSort),
                response2 -> {
                    assertThat(response2.getHits().getHits().length, equalTo(30));

                    // Verify second page starts after first page
                    long firstValuePage2 = (Long) response2.getHits().getHits()[0].getSortValues()[0];
                    long lastValuePage1 = (Long) lastSort[0];
                    assertThat(firstValuePage2, greaterThan(lastValuePage1));
                }
            );
        } finally {
            response1.decRef();
        }

        assertBusy(() -> {
            long currentBreaker = getRequestBreakerUsed(coordinatorNode);
            assertThat(
                "Coordinator circuit breaker should be released after paginated chunked fetches, current: "
                    + currentBreaker
                    + ", before: "
                    + breakerBefore,
                currentBreaker,
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    public void testChunkedFetchWithDfsQueryThenFetch() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        createIndexForTest(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        populateIndex(INDEX_NAME, 100, 5_000);
        ensureGreen(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);

        assertNoFailuresAndResponse(
            internalCluster().client(coordinatorNode)
                .prepareSearch(INDEX_NAME)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(matchAllQuery())
                .setSize(50)
                .addSort(SORT_FIELD, SortOrder.ASC),
            response -> {
                assertThat(response.getHits().getHits().length, equalTo(50));
                verifyHitsOrder(response);
            }
        );

        assertBusy(() -> {
            assertThat(
                "Coordinator circuit breaker should be released after DFS chunked fetch",
                getRequestBreakerUsed(coordinatorNode),
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    public void testChunkedFetchCircuitBreakerReleasedOnFailure() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        createIndexForTest(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        populateIndex(INDEX_NAME, 100, 5_000);
        ensureGreen(INDEX_NAME);

        long breakerBefore = getRequestBreakerUsed(coordinatorNode);

        // Execute search that will fail
        expectThrows(
            Exception.class,
            () -> internalCluster().client(coordinatorNode)
                .prepareSearch(INDEX_NAME)
                .setQuery(matchAllQuery())
                .setSize(50)
                .addSort("non_existent_field", SortOrder.ASC)
                .get()
        );

        assertBusy(() -> {
            assertThat(
                "Coordinator circuit breaker should be released even after chunked fetch failure",
                getRequestBreakerUsed(coordinatorNode),
                lessThanOrEqualTo(breakerBefore)
            );
        });
    }

    private void populateIndex(String indexName, int nDocs, int textSize) throws IOException {
        int batchSize = 50;
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
                                .field("large_text_1", Strings.repeat("large content field 1 ", textSize))
                                .field("large_text_2", Strings.repeat("large content field 2 ", textSize))
                                .field("large_text_3", Strings.repeat("large content field 3 ", textSize))
                                .field("keyword", "value" + (i % 10))
                                .endObject()
                        )
                );
            }
            indexRandom(batch == 0, builders);
        }
        refresh(indexName);
    }

    private void createIndexForTest(String indexName, Settings indexSettings) {
        assertAcked(
            prepareCreate(indexName).setSettings(indexSettings)
                .setMapping(
                    SORT_FIELD,
                    "type=long",
                    "text",
                    "type=text,store=true",
                    "large_text_1",
                    "type=text,store=false",
                    "large_text_2",
                    "type=text,store=false",
                    "large_text_3",
                    "type=text,store=false",
                    "keyword",
                    "type=keyword"
                )
        );
    }

    private long getRequestBreakerUsed(String node) {
        CircuitBreakerService breakerService = internalCluster().getInstance(CircuitBreakerService.class, node);
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        return breaker.getUsed();
    }

    private void verifyHitsOrder(SearchResponse response) {
        for (int i = 0; i < response.getHits().getHits().length - 1; i++) {
            long current = (Long) response.getHits().getHits()[i].getSortValues()[0];
            long next = (Long) response.getHits().getHits()[i + 1].getSortValues()[0];
            assertThat("Hits should be in ascending order", current, lessThan(next));
        }
    }
}
