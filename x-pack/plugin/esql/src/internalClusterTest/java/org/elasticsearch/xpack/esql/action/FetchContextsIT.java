/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.fetch.lifetime.FetchContextService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

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
    private final AtomicInteger freed = new AtomicInteger();
    private final Set<Long> keepAlivesMillis = ConcurrentHashMap.newKeySet();
    private int shards;
    private TimeValue keepAlive;

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
        keepAlive = TimeValue.timeValueSeconds(between(60, 600));
        updateClusterSettings(Settings.builder().put(FetchContextService.CONTEXT_KEEP_ALIVE.getKey(), keepAlive));
        // counts registered contexts only: the reader contexts of the eager path are never registered
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            MockSearchService mock = (MockSearchService) searchService;
            mock.setOnPutContext(context -> {
                registered.incrementAndGet();
                keepAlivesMillis.add(context.keepAlive());
            });
            mock.setOnRemoveContext(context -> {
                if (context.singleSession() == false) {
                    freed.incrementAndGet();
                }
            });
        }
    }

    @After
    public void resetCountersAndKeepAlive() {
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            MockSearchService mock = (MockSearchService) searchService;
            mock.setOnPutContext(context -> {});
            mock.setOnRemoveContext(context -> {});
        }
        updateClusterSettings(Settings.builder().putNull(FetchContextService.CONTEXT_KEEP_ALIVE.getKey()));
    }

    /**
     * Every data node registers a context per shard, with the keep-alive of {@link FetchContextService#CONTEXT_KEEP_ALIVE}.
     * The query returns the rows it returns without the fetch phase, and all the contexts are freed: by the data nodes
     * for shards without surviving rows, by the fetch for the shards it read, and by the coordinator's lease for the rest.
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
        assertThat("the keep-alive the coordinator sent", keepAlivesMillis, equalTo(Set.of(keepAlive.millis())));
        assertBusy(() -> {
            assertThat(freed.get(), equalTo(registered.get()));
            assertThat(indicesAdmin().prepareStats(INDEX).setSearch(true).get().getTotal().getSearch().getOpenContexts(), equalTo(0L));
        });
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
        return syncEsqlQueryRequest(QUERY).pragmas(
            new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_PHASE.getKey(), fetchPhase).build())
        );
    }
}
