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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.fetch.FetchRequest;
import org.elasticsearch.xpack.esql.fetch.FetchService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;

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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // counts the requests that free fetch contexts, and holds the fetch requests
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Before
    public void setupIndexAndCounters() {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assumeTrue("the fetch_phase pragma needs a snapshot build", canUseQueryPragmas());
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
     * without surviving rows, and by the fetch and the coordinator for the rest.
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
        // each data node opens its contexts with its own search.default_keep_alive, which the test cluster randomizes
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
     * When every row is a winner, the fetch reads every context that kept rows. A {@code TopN} cut reads each of them for
     * the last time, so the data nodes free them once they answered, and the coordinator sends no free request.
     */
    public void testTheLastFetchFreesTheContextsItRead() throws Exception {
        String everyRow = "FROM " + INDEX + " | SORT ts DESC | LIMIT 100 | KEEP a, b, ts";
        try (EsqlQueryResponse response = run(request(everyRow, true))) {
            assertThat(getValuesList(response), hasSize(60));
        }

        assertThat("a registered context per shard", registered.get(), equalTo(shards));
        assertBusy(() -> {
            assertThat(freed.get(), equalTo(registered.get()));
            assertThat(indicesAdmin().prepareStats(INDEX).setSearch(true).get().getTotal().getSearch().getOpenContexts(), equalTo(0L));
        });
        // a shard without documents keeps no rows, and its data node frees its context itself too
        assertThat(freeRequests.get(), equalTo(0));
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
     * With two cuts the fetch runs after the first, the cut closest to the relation, and the second cut runs over the
     * fetched rows. The contexts no row of the first cut reads are freed before the fetch, and the fetch reads the others.
     */
    public void testTheLosersOfTheFirstOfTwoCutsAreFreedBeforeTheFetch() throws Exception {
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
        // the 20 newest rows, fetched, then the oldest of them: one winner
        ActionFuture<EsqlQueryResponse> query = client().execute(
            EsqlQueryAction.INSTANCE,
            request("FROM " + INDEX + " | SORT ts DESC | LIMIT 20 | SORT ts ASC | LIMIT 1 | KEEP a, b, ts", true)
        );
        try {
            safeAwait(fetchArrived);
            assertBusy(
                () -> assertThat("only the contexts of the first cut stay open", Set.copyOf(registeredIds), equalTo(Set.copyOf(fetched)))
            );
        } finally {
            releaseFetches.countDown();
        }
        try (EsqlQueryResponse response = query.actionGet(30, TimeUnit.SECONDS)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("a-40", 400L, 40L))));
        }
        assertThat(fetched, not(empty()));
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
     * A plain {@code LIMIT} loads its columns eagerly even when the query asks for the fetch phase, so it keeps no context.
     */
    public void testAPlainLimitRegistersNoContext() {
        try (EsqlQueryResponse response = run(request("FROM " + INDEX + " | LIMIT 5 | KEEP a, b, ts", true))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(5));
            for (List<Object> row : rows) {
                long ts = (Long) row.get(2);
                assertThat(row, equalTo(List.of("a-" + ts, ts * 10, ts)));
            }
        }
        assertThat(registered.get(), equalTo(0));
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
