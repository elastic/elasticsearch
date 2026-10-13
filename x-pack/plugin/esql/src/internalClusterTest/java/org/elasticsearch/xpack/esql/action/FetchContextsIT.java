/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // counts the requests that free fetch contexts
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
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
     * When every row is a winner, the fetch reads every context that kept rows. The fetch frees none of them, so the
     * coordinator's lease frees them all once the query ends, with one request of the free action of {@code _search} per
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
     * Without the fetch phase the query reads through unregistered contexts, as it always did.
     */
    public void testQueryWithoutTheFetchPhaseRegistersNoContext() {
        try (EsqlQueryResponse response = run(request(false))) {
            assertThat(getValuesList(response), hasSize(10));
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
