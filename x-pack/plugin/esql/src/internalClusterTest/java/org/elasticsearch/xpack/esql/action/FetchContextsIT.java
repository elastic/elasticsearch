/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.fetch.FetchRequest;
import org.elasticsearch.xpack.esql.fetch.FetchService;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The reader contexts of the fetch phase, on a cluster with three data nodes. The data nodes of a query that gets the
 * fetch phase open a registered context per shard, the coordinator fetches from them, and every one of them is freed by
 * the time the query is over.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 3)
public class FetchContextsIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "fetch_contexts";
    private static final String QUERY = "FROM " + INDEX + " | SORT ts DESC | LIMIT 10 | KEEP a, b, ts";

    private final AtomicInteger registered = new AtomicInteger();
    private final Set<ShardSearchContextId> registeredIds = ConcurrentHashMap.newKeySet();
    private final AtomicInteger freed = new AtomicInteger();
    private final AtomicInteger freeRequests = new AtomicInteger();
    private final Set<Long> keepAlivesMillis = ConcurrentHashMap.newKeySet();
    private int shards;

    private final List<Runnable> removeMessageListeners = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // counts the requests that free fetch contexts, and sees the responses the coordinator receives
        plugins.add(MockTransportService.TestPlugin.class);
        // keeps a shard searching while the coordinator fetches
        plugins.add(SimplePauseFieldPlugin.class);
        return plugins;
    }

    @Before
    public void setupIndexAndCounters() {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assumeTrue("the fetch_phase pragma needs a snapshot build", canUseQueryPragmas());
        SimplePauseFieldPlugin.resetPlugin();
        shards = between(3, 6);
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(indexSettings(shards, 0))
                .setMapping("ts", "type=long", "a", "type=keyword", "b", "type=long")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 60; i++) {
            bulk.add(prepareIndex(INDEX).setSource(Map.of("ts", i, "a", "a-" + i, "b", i * 10L)));
        }
        assertFalse(bulk.get().hasFailures());
        ensureGreen(INDEX);
        // counts registered contexts only. The reader contexts of the eager path are never registered, but the mock reports
        // when they close, so a removal counts only for a registered id.
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            MockSearchService mock = (MockSearchService) searchService;
            mock.setOnPutContext(context -> {
                registered.incrementAndGet();
                registeredIds.add(context.id());
                keepAlivesMillis.add(context.keepAlive());
            });
            mock.setOnRemoveContext(context -> {
                if (registeredIds.remove(context.id())) {
                    freed.incrementAndGet();
                }
            });
        }
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) transportService).<TransportRequest>addRequestHandlingBehavior(
                SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME,
                (handler, request, channel, task) -> {
                    freeRequests.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                }
            );
        }
    }

    @After
    public void resetCounters() {
        SimplePauseFieldPlugin.release();
        removeMessageListeners.forEach(Runnable::run);
        removeMessageListeners.clear();
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            MockSearchService mock = (MockSearchService) searchService;
            mock.setOnPutContext(context -> {});
            mock.setOnRemoveContext(context -> {});
        }
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) transportService).clearAllRules();
        }
    }

    /**
     * Every data node registers a single-session context per shard, with the keep-alive of {@code _search}. The query
     * returns the rows it returns without the fetch phase, and all the contexts are freed: by the data nodes for shards
     * without surviving rows, and by the coordinator's lease for the rest.
     */
    public void testQueryWithTheFetchPhaseFreesItsContexts() throws Exception {
        List<List<Object>> eager;
        try (EsqlQueryResponse response = run(request(false))) {
            eager = getValuesList(response);
        }
        try (EsqlQueryResponse response = run(request(true))) {
            assertThat(getValuesList(response), equalTo(eager));
        }

        assertThat("a registered context per shard", registered.get(), equalTo(shards));
        // each data node keeps a context for its own search.default_keep_alive, which the test cluster randomizes
        Set<Long> searchKeepAlives = new HashSet<>();
        for (SearchService searchService : internalCluster().getDataNodeInstances(SearchService.class)) {
            searchKeepAlives.add(searchService.getDefaultKeepAliveInMillis());
        }
        assertThat("the keep-alive of _search", keepAlivesMillis, everyItem(in(searchKeepAlives)));
        assertBusy(() -> {
            assertThat(freed.get(), equalTo(registered.get()));
            assertThat(indicesAdmin().prepareStats(INDEX).setSearch(true).get().getTotal().getSearch().getOpenContexts(), equalTo(0L));
        });
    }

    /**
     * When every row is a winner, the fetch reads every context that kept rows. The fetch requests free none of them, so the
     * coordinator's lease frees them all once the fetch ended, with one request of the free action of {@code _search} per
     * context.
     */
    public void testTheLeaseFreesTheContextsAFetchRead() throws Exception {
        String everyRow = "FROM " + INDEX + " | SORT ts DESC | LIMIT 100 | KEEP a, b, ts";
        try (EsqlQueryResponse response = run(request(everyRow, true))) {
            assertThat(getValuesList(response), hasSize(60));
        }

        assertThat("a registered context per shard", registered.get(), equalTo(shards));
        assertBusy(() -> {
            assertThat(freed.get(), equalTo(registered.get()));
            assertThat(indicesAdmin().prepareStats(INDEX).setSearch(true).get().getTotal().getSearch().getOpenContexts(), equalTo(0L));
        });
        // a shard without documents keeps no rows, and its data node frees its context itself
        assertThat(freeRequests.get(), allOf(greaterThan(0), lessThanOrEqualTo(shards)));
    }

    /**
     * The contexts of shards whose rows all lost the cut of the coordinator are freed before the fetch loads the winners.
     */
    public void testLosersAreFreedBeforeTheFetch() throws Exception {
        Set<ShardSearchContextId> fetched = ConcurrentHashMap.newKeySet();
        CountDownLatch fetchArrived = new CountDownLatch(1);
        CountDownLatch releaseFetches = new CountDownLatch(1);
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) transportService).<FetchRequest>addRequestHandlingBehavior(
                FetchService.FETCH_ACTION_NAME,
                (handler, request, channel, task) -> {
                    request.shards().forEach(shard -> fetched.add(shard.contextId()));
                    fetchArrived.countDown();
                    safeAwait(releaseFetches);
                    handler.messageReceived(request, channel, task);
                }
            );
        }
        // one winner: every other node that sends a row to the coordinator holds a loser
        ActionFuture<EsqlQueryResponse> query = client().execute(
            EsqlQueryAction.INSTANCE,
            request("FROM " + INDEX + " | SORT ts DESC | LIMIT 1 | KEEP a, b, ts", true)
        );
        try {
            safeAwait(fetchArrived);
            assertBusy(() -> assertThat("only the winner stays open", Set.copyOf(registeredIds), equalTo(Set.copyOf(fetched))));
        } finally {
            releaseFetches.countDown();
        }
        try (EsqlQueryResponse response = query.actionGet(30, TimeUnit.SECONDS)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("a-59", 590L, 59L))));
        }
        assertThat(fetched, hasSize(1));
        assertBusy(() -> assertThat(freed.get(), equalTo(registered.get())));
    }

    /**
     * Without the fetch phase the query reads through unregistered contexts, as it always did.
     */
    public void testQueryWithoutTheFetchPhaseRegistersNoContext() {
        try (EsqlQueryResponse response = run(request(false))) {
            assertThat(getValuesList(response), hasSize(10));
        }
        assertThat(registered.get(), equalTo(0));
    }

    /**
     * Without a sort the coordinator fetches the rows a {@code LIMIT} keeps as soon as they arrive. The fast shard holds
     * fewer rows than the limit, so the query also waits for the slow shard, which searches on the same node. The
     * coordinator fetches the rows of the fast shard while that node still searches, the node answers the query only
     * afterwards, and every context is freed at the end.
     */
    public void testAPlainLimitFetchesWhileTheNodeStillSearches() throws Exception {
        List<String> dataNodes = new ArrayList<>();
        for (DiscoveryNode node : internalCluster().clusterService().state().nodes().getDataNodes().values()) {
            dataNodes.add(node.getName());
        }
        Collections.shuffle(dataNodes, random());
        String searching = dataNodes.get(0);
        String coordinator = dataNodes.get(1);
        String fast = "fetch_contexts_fast";
        String slow = "fetch_contexts_slow";
        assertAcked(
            indicesAdmin().prepareCreate(fast)
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", searching))
                .setMapping("ts", "type=long", "payload", "type=keyword", "pause", "type=long")
        );
        assertAcked(
            indicesAdmin().prepareCreate(slow)
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", searching))
                .setMapping("""
                    {
                      "runtime": { "pause": { "type": "long", "script": { "source": "", "lang": "pause" } } },
                      "properties": { "ts": { "type": "long" }, "payload": { "type": "keyword" } }
                    }""")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 20; i++) {
            bulk.add(prepareIndex(fast).setSource("ts", i, "payload", "payload-" + i, "pause", 0));
            bulk.add(prepareIndex(slow).setSource("ts", 100 + i, "payload", "payload-" + (100 + i)));
        }
        assertFalse(bulk.get().hasFailures());
        ensureGreen(fast, slow);

        CountDownLatch fetchAnswered = responseReceived(coordinator, FetchService.FETCH_ACTION_NAME, searching);
        CountDownLatch searchAnswered = responseReceived(coordinator, ComputeService.DATA_ACTION_NAME, searching);
        // more than the fast shard holds, so its rows never use up the limit that the drivers of the node share
        int limit = 30;
        EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM " + fast + "," + slow + " | WHERE pause >= 0 | LIMIT " + limit + " | KEEP ts, payload"
        ).pragmas(
            new QueryPragmas(
                Settings.builder()
                    .put(QueryPragmas.FETCH_PHASE.getKey(), true)
                    // a driver per shard, side by side, so the fast shard sends its rows while the slow one waits
                    .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 2)
                    .put(QueryPragmas.DATA_PARTITIONING.getKey(), "shard")
                    .build()
            )
        );
        ActionFuture<EsqlQueryResponse> query = client(coordinator).execute(EsqlQueryAction.INSTANCE, request);
        // asserted once the response is in hand, so a failure still closes it
        boolean slowShardWaits;
        boolean fetchAnsweredWhileTheNodeSearched;
        try {
            slowShardWaits = SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);
            fetchAnsweredWhileTheNodeSearched = fetchAnswered.await(30, TimeUnit.SECONDS) && searchAnswered.getCount() == 1;
        } finally {
            SimplePauseFieldPlugin.release();
        }
        try (EsqlQueryResponse response = query.actionGet(30, TimeUnit.SECONDS)) {
            assertTrue("the slow shard waits", slowShardWaits);
            assertTrue("the fetch answered while the node still searched", fetchAnsweredWhileTheNodeSearched);
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(limit));
            Set<Long> fastRows = new HashSet<>();
            for (List<Object> row : rows) {
                long ts = (Long) row.getFirst();
                assertThat(row.get(1), equalTo("payload-" + ts));
                if (ts < 20) {
                    assertTrue("each row once", fastRows.add(ts));
                }
            }
            assertThat("every row of the fast shard", fastRows, hasSize(20));
            assertFalse(response.isPartial());
        }
        assertBusy(() -> assertThat(freed.get(), equalTo(registered.get())));
    }

    /**
     * A plain {@code LIMIT} asks the nodes for rows one at a time here, and ends the exchange once it has its rows. The
     * nodes it reached free the contexts whose rows didn't leave them, and the lease frees the rest.
     */
    public void testAPlainLimitFreesTheContextsOfTheNodesItReached() throws Exception {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + INDEX + " | LIMIT 5 | KEEP a, b, ts").pragmas(
            new QueryPragmas(
                Settings.builder()
                    .put(QueryPragmas.FETCH_PHASE.getKey(), true)
                    .put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), 1)
                    .build()
            )
        );
        try (EsqlQueryResponse response = run(request)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(5));
            for (List<Object> row : rows) {
                long ts = (Long) row.get(2);
                assertThat(row, equalTo(List.of("a-" + ts, ts * 10, ts)));
            }
        }

        assertThat(registered.get(), allOf(greaterThan(0), lessThanOrEqualTo(shards)));
        assertBusy(() -> {
            assertThat(freed.get(), equalTo(registered.get()));
            assertThat(indicesAdmin().prepareStats(INDEX).setSearch(true).get().getTotal().getSearch().getOpenContexts(), equalTo(0L));
        });
    }

    /**
     * Counts down once {@code coordinator} receives the response of {@code node} to a request of {@code action}.
     */
    private CountDownLatch responseReceived(String coordinator, String action, String node) {
        CountDownLatch received = new CountDownLatch(1);
        TransportMessageListener listener = new TransportMessageListener() {
            @Override
            @SuppressWarnings("rawtypes")
            public void onResponseReceived(long requestId, Transport.ResponseContext context, int networkMessageSize) {
                if (context != null && context.action().equals(action) && context.connection().getNode().getName().equals(node)) {
                    received.countDown();
                }
            }
        };
        MockTransportService transport = (MockTransportService) internalCluster().getInstance(TransportService.class, coordinator);
        transport.addMessageListener(listener);
        removeMessageListeners.add(() -> transport.removeMessageListener(listener));
        return received;
    }

    private static EsqlQueryRequest request(boolean fetchPhase) {
        return request(QUERY, fetchPhase);
    }

    private static EsqlQueryRequest request(String query, boolean fetchPhase) {
        return syncEsqlQueryRequest(query).pragmas(
            new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_PHASE.getKey(), fetchPhase).build())
        );
    }
}
