/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A shard response that the coordinating node has no room for trips its request breaker while it arrives. The search then fails as a
 * whole with a 429, instead of the shard failing, which would return partial results with allow_partial_search_results.
 * <p>
 * One data node holds a {@value #SMALL} index and another a {@value #LARGE} one, whose responses don't fit on the coordinating node.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class SearchShardResponseCircuitBreakerIT extends ESIntegTestCase {

    private static final String SMALL = "small";
    private static final String LARGE = "large";
    private static final int LARGE_DOCS = 200;
    private static final String COORDINATOR_REQUEST_LIMIT = "50kb";

    private List<String> dataNodes;
    private String coordinator;

    private void startCluster() {
        // The hits of a large shard, about 600 KB, then arrive as the last chunk, embedded in the fetch response. Spread over several
        // chunks, they would trip the charge for the chunks instead.
        Settings settings = Settings.builder().put(SearchService.FETCH_PHASE_CHUNKED_TARGET_CHUNK_BYTES.getKey(), "64mb").build();
        dataNodes = internalCluster().startNodes(2, settings);
        coordinator = internalCluster().startCoordinatingOnlyNode(
            Settings.builder()
                .put(settings)
                .put("indices.breaker.request.type", "memory")
                .put("indices.breaker.request.limit", COORDINATOR_REQUEST_LIMIT)
                .build()
        );
        createIndex(SMALL, dataNodes.get(0));
        createIndex(LARGE, dataNodes.get(1));

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 10; i++) {
            bulk.add(prepareIndex(SMALL).setSource("tag", "small-" + i, "payload", "small"));
        }
        for (int i = 0; i < LARGE_DOCS; i++) {
            bulk.add(prepareIndex(LARGE).setSource("tag", i + randomAlphaOfLength(2000), "payload", randomAlphaOfLength(4000)));
        }
        assertNoFailures(bulk.get());
        refresh(SMALL, LARGE);
    }

    private void createIndex(String index, String node) {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", node)
            ).setMapping("tag", "type=keyword", "payload", "type=keyword,index=false,doc_values=false")
        );
        ensureGreen(index);
    }

    public void testQueryResponseTripsWhileArriving() throws Exception {
        startCluster();
        // Batching sends one response per data node instead of one per shard, through a different handler
        for (boolean batched : new boolean[] { true, false }) {
            updateClusterSettings(Settings.builder().put(SearchService.BATCHED_QUERY_PHASE.getKey(), batched));
            assertTripsWhileArriving(
                client(coordinator).prepareSearch(SMALL, LARGE)
                    .setSize(0)
                    .addAggregation(AggregationBuilders.terms("tags").field("tag").size(LARGE_DOCS)),
                "query",
                ShardResponseReservation.BREAKER_LABEL
            );
        }
    }

    public void testDfsQueryResponseTripsWhileArriving() throws Exception {
        startCluster();
        assertTripsWhileArriving(
            client(coordinator).prepareSearch(SMALL, LARGE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(0)
                .addAggregation(AggregationBuilders.terms("tags").field("tag").size(LARGE_DOCS)),
            DfsQueryPhase.NAME,
            ShardResponseReservation.BREAKER_LABEL
        );
    }

    public void testFetchResponseTripsWhileArriving() throws Exception {
        startCluster();
        // A chunked fetch goes through the coordinating node's own action, which has its own handler for the fetch response
        for (boolean chunked : new boolean[] { true, false }) {
            updateClusterSettings(Settings.builder().put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), chunked));
            assertTripsWhileArriving(
                client(coordinator).prepareSearch(SMALL, LARGE).setSize(LARGE_DOCS).setFetchSource(true),
                FetchSearchPhase.NAME,
                ShardResponseReservation.FETCH_BREAKER_LABEL
            );
        }
    }

    public void testScrollQueryResponseTripsWhileArriving() throws Exception {
        startCluster();
        // Sorting on the long tags makes the query responses large
        assertScrollTripsWhileArriving(
            prepareScroll(SMALL, LARGE).addSort("tag", SortOrder.ASC),
            "query",
            ShardResponseReservation.BREAKER_LABEL
        );
    }

    public void testScrollFetchResponseTripsWhileArriving() throws Exception {
        startCluster();
        assertScrollTripsWhileArriving(prepareScroll(SMALL, LARGE), FetchSearchPhase.NAME, ShardResponseReservation.FETCH_BREAKER_LABEL);
    }

    public void testScrollQueryAndFetchResponseTripsWhileArriving() throws Exception {
        startCluster();
        // A scroll over a single shard queries and fetches in one round trip
        assertScrollTripsWhileArriving(
            prepareScroll(LARGE).setPreference("_shards:0"),
            "query",
            ShardResponseReservation.FETCH_BREAKER_LABEL
        );
    }

    /**
     * Starts the scroll on a data node, whose breaker has room for the first round, so that only the next round goes through the
     * coordinating node.
     */
    private SearchRequestBuilder prepareScroll(String... indices) {
        return client(dataNodes.getFirst()).prepareSearch(indices).setScroll(TimeValue.timeValueMinutes(1)).setSize(LARGE_DOCS / 4);
    }

    private void assertScrollTripsWhileArriving(SearchRequestBuilder start, String phase, String label) throws Exception {
        SearchResponse firstRound = start.get();
        String scrollId = firstRound.getScrollId();
        firstRound.decRef();
        try {
            assertTripsWhileArriving(
                client(coordinator).prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(1)),
                phase,
                label
            );
        } finally {
            client().prepareClearScroll().addScrollId(scrollId).get();
        }
    }

    private void assertTripsWhileArriving(ActionRequestBuilder<?, SearchResponse> request, String phase, String label) throws Exception {
        // The search would return partial results if the trip failed the shard
        Exception e = expectThrows(Exception.class, request::get);
        assertThat(e, instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException failure = (SearchPhaseExecutionException) e;
        assertThat(failure.getPhaseName(), equalTo(phase));
        assertThat(failure.shardFailures(), emptyArray());
        assertThat(failure.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        CircuitBreakingException trip = (CircuitBreakingException) ExceptionsHelper.unwrap(e, CircuitBreakingException.class);
        assertThat(trip, notNullValue());
        assertThat(trip.getMessage(), containsString("[" + label + "]"));

        CircuitBreaker breaker = internalCluster().getInstance(CircuitBreakerService.class, coordinator).getBreaker(CircuitBreaker.REQUEST);
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(0L)));
    }
}
