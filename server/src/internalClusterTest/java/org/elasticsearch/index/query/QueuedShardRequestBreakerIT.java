/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchQueryThenFetchAsyncAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A data node charges what the {@code terms} values of a shard request decode to from when it reads the request, so they are charged
 * while the request waits in the search queue, not only once a shard builds its query.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class QueuedShardRequestBreakerIT extends ESIntegTestCase {

    private static final String INDEX = "test";
    // More than one, so that the data node gets one batched request for all of them
    private static final int SHARDS = 3;

    private String dataNode;
    private String coordinatingNode;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // The test cluster gives some nodes a noop request breaker, which charges nothing.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            // One search thread, so that blocking it queues the shard requests.
            .put("thread_pool.search.size", 1)
            .put(SearchService.BATCHED_QUERY_PHASE.getKey(), true)
            .build();
    }

    /**
     * The shards of a batched request share one copy of its values, so the copy is charged once while they wait in the queue, and released
     * once the search responds.
     */
    public void testQueuedShardRequestIsChargedForItsDecodedTerms() throws Exception {
        startCluster();
        TermsQueryBuilder query = termsQuery(20_000);
        long decodedBytes = decodedBytesOverWire(query);
        assertThat(decodedBytes, greaterThan(0L));

        CircuitBreaker breaker = requestBreaker(dataNode);
        long before = breaker.getUsed();
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        internalCluster().getInstance(ThreadPool.class, dataNode).executor(ThreadPool.Names.SEARCH).execute(() -> {
            blocked.countDown();
            // Longer than the assertBusy below, so that a missing charge fails there
            safeAwait(release, TimeValue.timeValueSeconds(30));
        });
        ActionFuture<SearchResponse> search;
        try {
            safeAwait(blocked);
            search = client(coordinatingNode).prepareSearch(INDEX).setQuery(query).execute();
            // One request carries every shard, so once it is read the charge cannot be on its way to more copies
            assertBusy(() -> {
                assertThat(requestsReceived(SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME), equalTo(1L));
                assertThat("charged once while queued", breaker.getUsed() - before, equalTo(decodedBytes));
            });
        } finally {
            release.countDown();
        }
        assertHitCount(search, 1L);
        assertThat(requestsReceived(SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME), equalTo(1L));
        assertThat("no shard request of its own", requestsReceived(SearchTransportService.QUERY_ACTION_NAME), equalTo(0L));
        assertBusy(() -> assertThat("released once the search responds", breaker.getUsed(), equalTo(before)));
    }

    /**
     * A shard request whose values trip the request breaker as the data node reads them fails with a 429 before it waits to run, and
     * leaves nothing charged.
     */
    public void testShardRequestTripsWhileRead() throws Exception {
        startCluster();
        TermsQueryBuilder large = termsQuery(60_000);
        // Over a chunk, so the breaker is checked while the values are read
        assertThat(decodedBytesOverWire(large), greaterThan(ByteSizeValue.ofMb(1).getBytes()));
        updateClusterSettings(Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "1mb"));
        try {
            CircuitBreaker breaker = requestBreaker(dataNode);
            long before = breaker.getUsed();
            long tripsBefore = breaker.getTrippedCount();
            // A small request passes the same limit, so the trip below is not one that every request would hit
            assertHitCount(client(coordinatingNode).prepareSearch(INDEX).setQuery(termsQuery(100)), 1L);

            CountDownLatch blocked = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            internalCluster().getInstance(ThreadPool.class, dataNode).executor(ThreadPool.Names.SEARCH).execute(() -> {
                blocked.countDown();
                safeAwait(release, TimeValue.timeValueSeconds(30));
            });
            try {
                safeAwait(blocked);
                // Fails while no shard can run, so it tripped while read, not once a shard built its query
                ActionFuture<SearchResponse> search = client(coordinatingNode).prepareSearch(INDEX).setQuery(large).execute();
                SearchPhaseExecutionException e = expectThrows(
                    SearchPhaseExecutionException.class,
                    () -> search.actionGet(TimeValue.timeValueSeconds(10))
                );
                assertThat(ExceptionsHelper.unwrap(e, CircuitBreakingException.class), notNullValue());
                assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
            } finally {
                release.countDown();
            }
            assertThat(breaker.getTrippedCount(), greaterThan(tripsBefore));
            assertBusy(() -> assertThat(breaker.getUsed(), equalTo(before)));
        } finally {
            updateClusterSettings(
                Settings.builder().putNull(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey())
            );
        }
    }

    /**
     * Starts a data node with every shard of the index and a coordinating node that sends it the shard requests over the transport layer,
     * since it holds no shards.
     */
    private void startCluster() {
        internalCluster().startMasterOnlyNode();
        dataNode = internalCluster().startDataOnlyNode();
        coordinatingNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARDS).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("id", "type=keyword")
        );
        prepareIndex(INDEX).setId("1").setSource("id", "id-0").get();
        refresh(INDEX);
    }

    private static TermsQueryBuilder termsQuery(int ids) {
        List<String> values = IntStream.range(0, ids).mapToObj(i -> "id-" + i).toList();
        return new TermsQueryBuilder("id", values);
    }

    private long requestsReceived(String action) {
        return internalCluster().getInstance(TransportService.class, dataNode).getRequestHandler(action).getStats().requestCount();
    }

    private static CircuitBreaker requestBreaker(String node) {
        return internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST);
    }

    /**
     * What the values of {@code query} take on a node that reads it beyond their serialized size.
     */
    private static long decodedBytesOverWire(TermsQueryBuilder query) throws IOException {
        NamedWriteableRegistry registry = internalCluster().getInstance(NamedWriteableRegistry.class);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(query);
            try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                return ((TermsQueryBuilder) in.readNamedWriteable(QueryBuilder.class)).getValues().decodedBytesOverWire();
            }
        }
    }
}
