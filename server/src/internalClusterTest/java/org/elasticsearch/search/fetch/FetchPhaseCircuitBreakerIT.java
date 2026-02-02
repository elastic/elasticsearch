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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests to verify that circuit breaker bytes are properly tracked and released
 * in the fetch phase across different search scenarios.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class FetchPhaseCircuitBreakerIT extends ESIntegTestCase {

    private static final String INDEX = "test_idx";
    private static final String SORT_FIELD = "sort_field";

    public void testSimpleFetchReleasesCircuitBreaker() throws Exception {
        String dataNode = startDataNode("100mb");
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertThat(internalCluster().size(), equalTo(2));

        createIndexForTest(
            INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        populateIndex(INDEX, 50, 10_000);
        ensureSearchable(INDEX);

        long breakerBeforeSearch = getRequestBreakerUsed(dataNode);

        assertNoFailuresAndResponse(
            client(coordinatorNode).prepareSearch(INDEX).setQuery(matchAllQuery()).setSize(10),
            response -> assertThat(response.getHits().getHits().length, equalTo(10))
        );

        assertBusy(() -> {
            assertThat(
                "Circuit breaker should be released after search completes",
                getRequestBreakerUsed(dataNode),
                lessThanOrEqualTo(breakerBeforeSearch)
            );
        });
    }

    public void testMultiShardSearchReleasesCircuitBreaker() throws Exception {
        String dataNode = startDataNode("100mb");
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertThat(internalCluster().size(), equalTo(2));

        createIndexForTest(
            INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        populateIndex(INDEX, 100, 10_000);  // More docs to spread across shards
        ensureSearchable(INDEX);

        long breakerBeforeSearch = getRequestBreakerUsed(dataNode);

        assertNoFailuresAndResponse(client(coordinatorNode).prepareSearch(INDEX).setQuery(matchAllQuery()).setSize(50), response -> {
            assertThat(response.getHits().getHits().length, equalTo(50));
        });

        assertBusy(() -> {
            assertThat(
                "Circuit breaker should be released after multi-shard search completes",
                getRequestBreakerUsed(dataNode),
                lessThanOrEqualTo(breakerBeforeSearch)
            );
        });
    }

    public void testMultipleSearchesNoMemoryLeak() throws Exception {
        String dataNode = startDataNode("100mb");
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertThat(internalCluster().size(), equalTo(2));

        createIndexForTest(
            INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        populateIndex(INDEX, 50, 10_000);
        ensureSearchable(INDEX);

        long breakerBeforeSearch = getRequestBreakerUsed(dataNode);

        // Execute multiple searches
        for (int i = 0; i < 100; i++) {
            assertNoFailuresAndResponse(
                client(coordinatorNode).prepareSearch(INDEX).setQuery(matchAllQuery()).setSize(10),
                response -> assertThat(response.getHits().getHits().length, equalTo(10))
            );
        }

        assertBusy(() -> {
            assertThat(
                "Circuit breaker should not grow after multiple searches (no leaks)",
                getRequestBreakerUsed(dataNode),
                lessThanOrEqualTo(breakerBeforeSearch)
            );
        });
    }

    public void testScrollSearchReleasesCircuitBreaker() throws Exception {
        String dataNode = startDataNode("100mb");
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertThat(internalCluster().size(), equalTo(2));

        createIndexForTest(
            INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        populateIndex(INDEX, 50, 10_000);
        ensureSearchable(INDEX);

        long breakerBeforeSearch = getRequestBreakerUsed(dataNode);

        // Initial scroll request - use coordinator node
        SearchResponse searchResponse = client(coordinatorNode).prepareSearch(INDEX)
            .setQuery(matchAllQuery())
            .setSize(10)
            .setScroll(TimeValue.timeValueMinutes(1))
            .get();

        String scrollId = searchResponse.getScrollId();
        assertNotNull(scrollId);
        assertThat(searchResponse.getHits().getHits().length, equalTo(10));
        searchResponse.decRef();

        try {
            // Scroll through results - use coordinator node
            for (int i = 0; i < 3; i++) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueMinutes(1));
                searchResponse = client(coordinatorNode).searchScroll(scrollRequest).actionGet();
                scrollId = searchResponse.getScrollId();
                searchResponse.decRef();
            }
        } finally {
            if (scrollId != null) {
                // Clear scroll - use coordinator node
                client(coordinatorNode).prepareClearScroll().addScrollId(scrollId).get();
            }
        }

        assertBusy(() -> {
            assertThat(
                "Circuit breaker should be released after scroll completes",
                getRequestBreakerUsed(dataNode),
                lessThanOrEqualTo(breakerBeforeSearch)
            );
        });
    }

    public void testPointInTimeSearchReleasesCircuitBreaker() throws Exception {
        String dataNode = startDataNode("100mb");
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertThat(internalCluster().size(), equalTo(2));

        createIndexForTest(
            INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        populateIndex(INDEX, 50, 10_000);
        ensureSearchable(INDEX);

        long breakerBeforeSearch = getRequestBreakerUsed(dataNode);

        var pitResponse = client(coordinatorNode).execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(INDEX).keepAlive(TimeValue.timeValueMinutes(1))
        ).actionGet();

        try {
            // Execute searches with PIT - use coordinator node
            for (int i = 0; i < 5; i++) {
                assertNoFailuresAndResponse(
                    client(coordinatorNode).prepareSearch()
                        .setPointInTime(new PointInTimeBuilder(pitResponse.getPointInTimeId()))
                        .setSize(10),
                    response -> {
                        assertThat(response.getHits().getHits().length, equalTo(10));
                    }
                );
            }
        } finally {
            // Close PIT - use coordinator node
            client(coordinatorNode).execute(
                TransportClosePointInTimeAction.TYPE,
                new ClosePointInTimeRequest(pitResponse.getPointInTimeId())
            ).actionGet();
        }

        assertBusy(() -> {
            assertThat(
                "Circuit breaker should be released after PIT searches complete",
                getRequestBreakerUsed(dataNode),
                lessThanOrEqualTo(breakerBeforeSearch)
            );
        });
    }

    public void testCircuitBreakerReleasedOnException() throws Exception {
        String dataNode = startDataNode("100mb");
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertThat(internalCluster().size(), equalTo(2));

        createIndexForTest(
            INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        populateIndex(INDEX, 50, 10_000);
        ensureSearchable(INDEX);

        long breakerBeforeSearch = getRequestBreakerUsed(dataNode);

        expectThrows(
            Exception.class,
            () -> client(coordinatorNode).prepareSearch(INDEX)
                .setQuery(matchAllQuery())
                .addScriptField(
                    "failing_script",
                    new Script(ScriptType.INLINE, "painless", "throw new RuntimeException('fetch failure')", Collections.emptyMap())
                )
                .setSize(10)
                .get()
        );

        assertBusy(() -> {
            assertThat(
                "Circuit breaker should be released even after exception",
                getRequestBreakerUsed(dataNode),
                lessThanOrEqualTo(breakerBeforeSearch)
            );
        });
    }

    public void testSearchAfterReleasesCircuitBreaker() throws Exception {
        String dataNode = startDataNode("100mb");
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertThat(internalCluster().size(), equalTo(2));

        createIndexForTest(
            INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        populateIndex(INDEX, 50, 10_000);
        ensureSearchable(INDEX);

        long breakerBeforeSearch = getRequestBreakerUsed(dataNode);

        // First page
        SearchResponse response1 = client(coordinatorNode).prepareSearch(INDEX)
            .setQuery(matchAllQuery())
            .setSize(10)
            .addSort(SORT_FIELD, SortOrder.ASC)
            .get();

        try {
            assertThat(response1.getHits().getHits().length, equalTo(10));
            Object[] sortValues = response1.getHits().getHits()[9].getSortValues();

            // Second page using search_after
            assertNoFailuresAndResponse(
                client(coordinatorNode).prepareSearch(INDEX)
                    .setQuery(matchAllQuery())
                    .setSize(10)
                    .addSort(SORT_FIELD, SortOrder.ASC)
                    .searchAfter(sortValues),
                response2 -> {
                    assertThat(response2.getHits().getHits().length, greaterThan(0));
                }
            );
        } finally {
            response1.decRef();
        }

        assertBusy(() -> {
            assertThat(
                "Circuit breaker should be released after search_after completes",
                getRequestBreakerUsed(dataNode),
                lessThanOrEqualTo(breakerBeforeSearch)
            );
        });
    }

    public void testCircuitBreakerTripsOnLargeFetch() throws Exception {
        // Use a very small circuit breaker limit to trigger trip
        String dataNode = startDataNode("50kb");
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertThat(internalCluster().size(), equalTo(2));

        createIndexForTest(
            INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        populateIndex(INDEX, 50, 10_000);
        ensureSearchable(INDEX);

        long breakerBeforeSearch = getRequestBreakerUsed(dataNode);

        // Search should trip the circuit breaker
        Exception exception = expectThrows(
            Exception.class,
            () -> client(coordinatorNode).prepareSearch(INDEX).setQuery(matchAllQuery()).setSize(50).get()
        );

        assertThat(
            "Should contain CircuitBreakingException",
            ExceptionsHelper.unwrap(exception, CircuitBreakingException.class),
            notNullValue()
        );

        assertThat(
            "Circuit breaking should map to 429 TOO_MANY_REQUESTS",
            ExceptionsHelper.status(exception),
            equalTo(RestStatus.TOO_MANY_REQUESTS)
        );

        assertBusy(() -> {
            assertThat(
                "Circuit breaker should be released after tripped search",
                getRequestBreakerUsed(dataNode),
                lessThanOrEqualTo(breakerBeforeSearch)
            );
        });
    }

    public void testCircuitBreakerTripsOnScrollFetch() throws Exception {
        String dataNode = startDataNode("50kb");
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertThat(internalCluster().size(), equalTo(2));

        createIndexForTest(
            INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        populateIndex(INDEX, 50, 10_000);
        ensureSearchable(INDEX);

        long breakerBeforeSearch = getRequestBreakerUsed(dataNode);

        var exception = expectThrows(
            Exception.class,
            () -> client(coordinatorNode).prepareSearch(INDEX)
                .setQuery(matchAllQuery())
                .setSize(50)
                .setScroll(TimeValue.timeValueMinutes(1))
                .get()
        );

        assertThat(
            "Should contain CircuitBreakingException",
            ExceptionsHelper.unwrap(exception, CircuitBreakingException.class),
            notNullValue()
        );

        assertThat(
            "Circuit breaking should map to 429 TOO_MANY_REQUESTS",
            ExceptionsHelper.status(exception),
            equalTo(RestStatus.TOO_MANY_REQUESTS)
        );

        assertBusy(() -> {
            assertThat(
                "Circuit breaker should be released after tripped scroll",
                getRequestBreakerUsed(dataNode),
                lessThanOrEqualTo(breakerBeforeSearch)
            );
        });
    }

    private String startDataNode(String cbRequestLimit) {
        return internalCluster().startNode(
            Settings.builder().put("indices.breaker.request.type", "memory").put("indices.breaker.request.limit", cbRequestLimit).build()
        );
    }

    private long getRequestBreakerUsed(String node) {
        CircuitBreakerService breakerService = internalCluster().getInstance(CircuitBreakerService.class, node);
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        return breaker.getUsed();
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
                    "keyword",
                    "type=keyword"
                )
        );
    }

    private void populateIndex(String indexName, int nDocs, int textSize) throws IOException {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < nDocs; i++) {
            builders.add(
                prepareIndex(indexName).setId(Integer.toString(i))
                    .setSource(
                        jsonBuilder().startObject()
                            .field(SORT_FIELD, i)
                            .field("text", "document " + i)
                            .field("large_text_1", Strings.repeat("large content field 1 ", textSize))
                            .field("large_text_2", Strings.repeat("large content field 2 ", textSize))
                            .field("keyword", "value" + (i % 10))
                            .endObject()
                    )
            );
        }
        indexRandom(true, builders);
    }
}
