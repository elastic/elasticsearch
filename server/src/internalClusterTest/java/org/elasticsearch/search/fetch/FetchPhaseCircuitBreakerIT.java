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
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration tests to verify that circuit breaker bytes are properly tracked and released
 * in the fetch phase across different search scenarios.
 */
@ESIntegTestCase.SuiteScopeTestCase
public class FetchPhaseCircuitBreakerIT extends ESIntegTestCase {

    private static final String INDEX_SMALL = "small_fetch_idx";
    private static final String INDEX_LARGE = "large_fetch_idx";
    private static final String SORT_FIELD = "sort_field";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("indices.breaker.request.type", "memory") // Use memory-based circuit breaker for accurate tracking
            .put("indices.breaker.request.limit", "50mb") // Set a reasonable limit for testing
            .build();
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createSmallIndex();
        ensureSearchable(INDEX_SMALL);
    }

    private void createSmallIndex() throws IOException {
        createIndex(INDEX_SMALL);
        populateIndex(INDEX_SMALL, 50, 10_000);
    }

    private void createLargeIndex() throws IOException {
        createIndex(INDEX_LARGE);
        populateIndex(INDEX_LARGE, 50, 100_000);
    }

    private void createIndex(String indexName) {
        assertAcked(
            prepareCreate(indexName).setMapping(
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
                            .field("large_text_3", Strings.repeat("large content field 3 ", textSize))
                            .field("keyword", "value" + (i % 10))
                            .endObject()
                    )
            );
        }
        indexRandom(true, builders);
    }

    /**
     * Test that circuit breaker bytes are properly released after a simple fetch phase.
     */
    public void testSimpleFetchReleasesCircuitBreaker() {
        long breakerBeforeSearch = getRequestBreakerUsed();

        assertNoFailuresAndResponse(
            prepareSearch(INDEX_SMALL).setQuery(matchAllQuery()).setSize(10),
            response -> assertThat(response.getHits().getHits().length, equalTo(10))
        );

        assertThat(
            "Circuit breaker should be released after search completes",
            getRequestBreakerUsed(),
            lessThanOrEqualTo(breakerBeforeSearch)
        );
    }

    /**
     * Test that circuit breaker bytes are released across multiple consecutive searches.
     * This verifies there are no memory leaks.
     */
    public void testMultipleSearchesNoMemoryLeak() {
        long initialBreaker = getRequestBreakerUsed();

        // Execute multiple searches
        for (int i = 0; i < 100; i++) {
            assertNoFailuresAndResponse(
                prepareSearch(INDEX_SMALL).setQuery(matchAllQuery()).setSize(10),
                response -> assertThat(response.getHits().getHits().length, equalTo(10))
            );
        }

        assertThat(
            "Circuit breaker should not grow after multiple searches (no leaks)",
            getRequestBreakerUsed(),
            lessThanOrEqualTo(initialBreaker + ByteSizeValue.ofKb(100).getBytes()) // Allow small variance
        );
    }

    /**
     * Test circuit breaker release with scroll search (scroll fetch path).
     */
    public void testScrollSearchReleasesCircuitBreaker() {
        long breakerBeforeSearch = getRequestBreakerUsed();

        // Initial scroll request
        SearchResponse searchResponse = prepareSearch(INDEX_SMALL).setQuery(matchAllQuery())
            .setSize(10)
            .setScroll(TimeValue.timeValueMinutes(1))
            .get();

        String scrollId = searchResponse.getScrollId();
        assertNotNull(scrollId);
        assertThat(searchResponse.getHits().getHits().length, equalTo(10));
        searchResponse.decRef();

        try {
            // Scroll through results
            for (int i = 0; i < 3; i++) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueMinutes(1));
                searchResponse = client().searchScroll(scrollRequest).actionGet();
                scrollId = searchResponse.getScrollId();
                searchResponse.decRef();
            }
        } finally {
            if (scrollId != null) {
                clearScroll(scrollId);
            }
        }

        assertThat(
            "Circuit breaker should be released after scroll completes",
            getRequestBreakerUsed(),
            lessThanOrEqualTo(breakerBeforeSearch + ByteSizeValue.ofKb(50).getBytes())
        );
    }

    /**
     * Test circuit breaker release with Point-in-Time search.
     */
    public void testPointInTimeSearchReleasesCircuitBreaker() throws IOException {
        long breakerBeforeSearch = getRequestBreakerUsed();

        var pitResponse = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(INDEX_SMALL).keepAlive(TimeValue.timeValueMinutes(1))
        ).actionGet();

        try {
            // Execute searches with PIT
            for (int i = 0; i < 5; i++) {
                assertNoFailuresAndResponse(
                    prepareSearch().setPointInTime(new PointInTimeBuilder(pitResponse.getPointInTimeId())).setSize(10),
                    response -> {
                        assertThat(response.getHits().getHits().length, equalTo(10));
                    }
                );
            }
        } finally {
            // Close PIT
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitResponse.getPointInTimeId())).actionGet();
        }

        assertThat(
            "Circuit breaker should be released after PIT searches complete",
            getRequestBreakerUsed(),
            lessThanOrEqualTo(breakerBeforeSearch + ByteSizeValue.ofKb(50).getBytes())
        );
    }

    /**
     * Test circuit breaker with single-shard search (combined query+fetch path).
     */
    public void testSingleShardSearchReleasesCircuitBreaker() throws IOException {
        String singleShardIndex = "single_shard_idx";
        assertAcked(
            prepareCreate(singleShardIndex).setSettings(
                Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
            ).setMapping(SORT_FIELD, "type=long", "text", "type=text")
        );

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            builders.add(
                prepareIndex(singleShardIndex).setId(Integer.toString(i)).setSource(SORT_FIELD, i, "text", Strings.repeat("content ", 1000))
            );
        }
        indexRandom(true, builders);
        ensureSearchable(singleShardIndex);

        long breakerBeforeSearch = getRequestBreakerUsed();

        assertNoFailuresAndResponse(prepareSearch(singleShardIndex).setQuery(matchAllQuery()).setSize(10), response -> {
            assertThat(response.getHits().getHits().length, equalTo(10));
        });

        assertThat(
            "Circuit breaker should be released after single-shard search completes",
            getRequestBreakerUsed(),
            lessThanOrEqualTo(breakerBeforeSearch)
        );
    }

    /**
     * Test circuit breaker release when search fails with an exception.
     */
    public void testCircuitBreakerReleasedOnException() {
        long breakerBeforeSearch = getRequestBreakerUsed();

        expectThrows(
            Exception.class,
            () -> prepareSearch(INDEX_SMALL).setQuery(matchAllQuery())
                .addSort("non_existent_field", SortOrder.ASC) // This will cause an error
                .get()
        );

        assertThat(
            "Circuit breaker should be released even after exception",
            getRequestBreakerUsed(),
            lessThanOrEqualTo(breakerBeforeSearch + ByteSizeValue.ofKb(10).getBytes())
        );
    }

    /**
     * Test circuit breaker with search after query (pagination).
     */
    public void testSearchAfterReleasesCircuitBreaker() {
        long breakerBeforeSearch = getRequestBreakerUsed();

        // First page
        SearchResponse response1 = prepareSearch(INDEX_SMALL).setQuery(matchAllQuery())
            .setSize(10)
            .addSort(SORT_FIELD, SortOrder.ASC)
            .get();

        assertThat(response1.getHits().getHits().length, equalTo(10));
        Object[] sortValues = response1.getHits().getHits()[9].getSortValues();

        // Second page using search_after
        assertNoFailuresAndResponse(
            prepareSearch(INDEX_SMALL).setQuery(matchAllQuery()).setSize(10).addSort(SORT_FIELD, SortOrder.ASC).searchAfter(sortValues),
            response2 -> {
                assertThat(response2.getHits().getHits().length, greaterThan(0));
            }
        );

        assertThat(
            "Circuit breaker should be released after search_after completes",
            getRequestBreakerUsed(),
            lessThanOrEqualTo(breakerBeforeSearch)
        );
    }

    private long getRequestBreakerUsed() {
        CircuitBreakerService breakerService = internalCluster().getInstance(CircuitBreakerService.class);
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        return breaker.getUsed();
    }
}
