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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class, MockSearchService.TestPlugin.class);
    }

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

    /**
     * The breaker trips after the coordinator decided to send a shard request, but before it was sent. Every reader context opened
     * on a data node must be released, so the search must fail only once that request is sent.
     * For the reasoning, see {@code AbstractSearchAsyncAction#deferCoordinatorTripFailure}.
     */
    public void testTripWhileSendingShardRequestLeavesNoReaderContext() throws Exception {
        dataNodes = internalCluster().startNodes(2);
        coordinator = internalCluster().startCoordinatingOnlyNode(
            Settings.builder().put("indices.breaker.request.type", "memory").put("indices.breaker.request.limit", "1b").build()
        );
        createIndex(SMALL, dataNodes.get(0));
        createIndex(LARGE, dataNodes.get(1));

        CircuitBreaker breaker = internalCluster().getInstance(CircuitBreakerService.class, coordinator).getBreaker(CircuitBreaker.REQUEST);
        long tripsBefore = breaker.getTrippedCount();
        PlainActionFuture<SearchResponse> search = new PlainActionFuture<>();
        AtomicReference<Thread> sender = new AtomicReference<>();
        AtomicReference<Runnable> firstRequest = new AtomicReference<>();
        AtomicBoolean heldSecondRequest = new AtomicBoolean();
        MockTransportService transportService = MockTransportService.getInstance(coordinator);
        // The first request is only sent from the hold below. Otherwise its response could trip before the sender thread checks for a
        // trip ahead of the second request, and the second request would never be sent.
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(SearchTransportService.DFS_ACTION_NAME) && sender.compareAndSet(null, Thread.currentThread())) {
                firstRequest.set(() -> {
                    try {
                        connection.sendRequest(requestId, action, request, options);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                return;
            }
            connection.sendRequest(requestId, action, request, options);
        });
        // The thread that sent the first request holds up the next one, after its trip check. Every response trips, so this thread
        // sends the first request, waits for it to trip, then waits up to 1s for a search that fails immediately to complete. With the
        // fix that wait always times out; it only bounds how long a regression has to show. The unit tests in
        // SearchQueryThenFetchAsyncActionTests cover the same order without timing.
        transportService.addGetConnectionBehavior((connectionManager, node) -> {
            if (Thread.currentThread() == sender.get() && heldSecondRequest.compareAndSet(false, true)) {
                firstRequest.get().run();
                waitUntil(() -> breaker.getTrippedCount() > tripsBefore);
                waitUntil(search::isDone, 1, TimeUnit.SECONDS);
            }
            return connectionManager.getConnection(node);
        });

        try {
            client(coordinator).prepareSearch(SMALL, LARGE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setMaxConcurrentShardRequests(SearchRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS)
                // Without a can_match round, the requests are sent from the thread that starts the search, not a transport thread
                .setPreFilterShardSize(Integer.MAX_VALUE)
                .execute(search);
            Exception e = expectThrows(Exception.class, () -> search.actionGet(SAFE_AWAIT_TIMEOUT));
            assertThat(ExceptionsHelper.unwrap(e, CircuitBreakingException.class), notNullValue());
            assertTrue("the second shard request was never held", heldSecondRequest.get());
            // The second response trips too, once its reader context is open. The trip skips the shards after the second.
            assertBusy(() -> assertThat(breaker.getTrippedCount(), equalTo(tripsBefore + 2)));
            assertBusy(MockSearchService::assertNoInFlightContext);
        } finally {
            transportService.clearAllRules();
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
