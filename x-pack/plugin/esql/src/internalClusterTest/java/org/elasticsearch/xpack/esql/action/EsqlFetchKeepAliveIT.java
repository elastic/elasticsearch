/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.fetch.FetchContextService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * The keep-alive of the fetch contexts of a query, on nodes that keep a context open for a short time and reap expired
 * contexts often, so a test sees a context expire without waiting for minutes. Each test starts the nodes it needs.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class EsqlFetchKeepAliveIT extends AbstractEsqlIntegTestCase {
    private static final TimeValue KEEP_ALIVE = TimeValue.timeValueSeconds(1);
    private static final String FAST = "keep_alive_fast";
    private static final String SLOW = "keep_alive_slow";

    private final AtomicInteger registered = new AtomicInteger();
    private final Set<ShardSearchContextId> registeredIds = ConcurrentHashMap.newKeySet();
    private final AtomicInteger freed = new AtomicInteger();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // counts the keep-alive requests, and loses the frees
        plugins.add(MockTransportService.TestPlugin.class);
        // keeps the slow shard searching while the fast node waits
        plugins.add(SimplePauseFieldPlugin.class);
        return plugins;
    }

    @Before
    public void checkTheFetchPhaseRuns() {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assumeTrue("the fetch_phase pragma needs a snapshot build", canUseQueryPragmas());
        SimplePauseFieldPlugin.resetPlugin();
    }

    @After
    public void releaseThePause() {
        SimplePauseFieldPlugin.release();
    }

    /**
     * A node that finished long before a slow one keeps the contexts of its winners, because the coordinator refreshes
     * them while the query waits. The slow node waits for three keep-alives, and the fetch reads every winner of the fast
     * node.
     */
    public void testAQueryThatWaitsLongerThanTheKeepAliveStillFetches() throws Exception {
        internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3, keepAlive(KEEP_ALIVE));
        countContexts();
        AtomicInteger keepAlives = countKeepAlives();
        createFastAndSlowIndices(dataNodes.get(0), dataNodes.get(1));

        ActionFuture<EsqlQueryResponse> query = client(dataNodes.get(2)).execute(EsqlQueryAction.INSTANCE, topTenOfBoth());
        boolean slowShardWaits;
        try {
            slowShardWaits = SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);
            // the contexts of the fast node expire unless the coordinator refreshes them
            safeSleep(KEEP_ALIVE.millis() * 3);
        } finally {
            SimplePauseFieldPlugin.release();
        }
        try (EsqlQueryResponse response = query.actionGet(30, TimeUnit.SECONDS)) {
            assertTrue("the slow shard waits", slowShardWaits);
            assertTheWinnersOfTheFastIndex(response);
        }
        assertThat("the coordinator refreshed the contexts", keepAlives.get(), greaterThan(0));
        assertBusy(() -> assertThat(freed.get(), equalTo(registered.get())));
    }

    /**
     * A data node whose default keep-alive is shorter than a third of the coordinator's keeps its contexts until the first
     * refresh, because it opens them with the keep-alive of the query.
     */
    public void testANodeWithAShorterKeepAliveKeepsItsContextsUntilTheFirstRefresh() throws Exception {
        internalCluster().startMasterOnlyNode();
        String coordinator = internalCluster().startDataOnlyNode(keepAlive(TimeValue.timeValueSeconds(3)));
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2, keepAlive(TimeValue.timeValueMillis(300)));
        countContexts();
        createFastAndSlowIndices(dataNodes.get(0), dataNodes.get(1));

        ActionFuture<EsqlQueryResponse> query = client(coordinator).execute(EsqlQueryAction.INSTANCE, topTenOfBoth());
        boolean slowShardWaits;
        try {
            slowShardWaits = SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);
            // longer than the keep-alive of the fast node, and past the first refresh of the coordinator
            safeSleep(2000);
        } finally {
            SimplePauseFieldPlugin.release();
        }
        try (EsqlQueryResponse response = query.actionGet(30, TimeUnit.SECONDS)) {
            assertTrue("the slow shard waits", slowShardWaits);
            assertTheWinnersOfTheFastIndex(response);
        }
        assertBusy(() -> assertThat(freed.get(), equalTo(registered.get())));
    }

    /**
     * The contexts of a query whose free requests got lost expire once their keep-alive passed, because nobody refreshes
     * them after the query ended.
     */
    public void testTheContextsOfAQueryWhoseFreeIsLostStillExpire() throws Exception {
        internalCluster().startMasterOnlyNode(keepAlive(KEEP_ALIVE));
        // the coordinator picks the keep-alive of the contexts, so it must be a node with the short one
        String coordinator = randomFrom(internalCluster().startDataOnlyNodes(between(1, 3), keepAlive(KEEP_ALIVE)));
        countContexts();
        String index = "keep_alive_lost";
        assertAcked(indicesAdmin().prepareCreate(index).setSettings(indexSettings(between(1, 3), 0)).setMapping("ts", "type=long"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 40; i++) {
            bulk.add(prepareIndex(index).setSource("ts", i, "payload", "payload-" + i));
        }
        assertFalse(bulk.get().hasFailures());
        ensureGreen(index);
        AtomicInteger lostFrees = new AtomicInteger();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) transportService).<TransportRequest>addRequestHandlingBehavior(
                SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME,
                (handler, request, channel, task) -> {
                    lostFrees.incrementAndGet();
                    channel.sendResponse(new ElasticsearchException("the free got lost"));
                }
            );
        }

        EsqlQueryRequest request = fetchPhase("FROM " + index + " | SORT ts DESC | LIMIT 5 | KEEP ts, payload");
        try (EsqlQueryResponse response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS)) {
            assertThat(getValuesList(response), hasSize(5));
        }

        assertBusy(() -> assertThat("the coordinator tried to free them", lostFrees.get(), greaterThan(0)));
        assertThat("their frees got lost", registeredIds, not(empty()));
        assertBusy(() -> assertThat("the reaper freed them", freed.get(), equalTo(registered.get())), 30, TimeUnit.SECONDS);
    }

    private static Settings keepAlive(TimeValue keepAlive) {
        return Settings.builder()
            .put(SearchService.DEFAULT_KEEPALIVE_SETTING.getKey(), keepAlive)
            // frees an expired context soon after its keep-alive passed
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(50))
            .build();
    }

    /**
     * Counts the contexts that every node registers and frees from now on.
     */
    private void countContexts() {
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            MockSearchService mock = (MockSearchService) searchService;
            mock.setOnPutContext(context -> {
                registered.incrementAndGet();
                registeredIds.add(context.id());
            });
            mock.setOnRemoveContext(context -> {
                if (registeredIds.remove(context.id())) {
                    freed.incrementAndGet();
                }
            });
        }
    }

    /**
     * Counts the keep-alive requests that every node receives from now on.
     */
    private AtomicInteger countKeepAlives() {
        AtomicInteger keepAlives = new AtomicInteger();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) transportService).<TransportRequest>addRequestHandlingBehavior(
                FetchContextService.KEEP_ALIVE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    keepAlives.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                }
            );
        }
        return keepAlives;
    }

    /**
     * A fast index on {@code fastNode} with the newest documents, and a slow index on {@code slowNode} whose runtime field
     * pauses its search until the test releases it.
     */
    private void createFastAndSlowIndices(String fastNode, String slowNode) {
        assertAcked(
            indicesAdmin().prepareCreate(FAST)
                .setSettings(indexSettings(between(1, 3), 0).put("index.routing.allocation.require._name", fastNode))
                .setMapping("ts", "type=long", "payload", "type=keyword", "pause", "type=long")
        );
        assertAcked(
            indicesAdmin().prepareCreate(SLOW)
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", slowNode))
                .setMapping("""
                    {
                      "runtime": { "pause": { "type": "long", "script": { "source": "", "lang": "pause" } } },
                      "properties": { "ts": { "type": "long" }, "payload": { "type": "keyword" } }
                    }""")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 20; i++) {
            bulk.add(prepareIndex(FAST).setSource("ts", 100 + i, "payload", "payload-" + (100 + i), "pause", 0));
            bulk.add(prepareIndex(SLOW).setSource("ts", i, "payload", "payload-" + i));
        }
        assertFalse(bulk.get().hasFailures());
        ensureGreen(FAST, SLOW);
    }

    /**
     * The ten newest documents, all in the fast index. The query waits for the slow index anyway, because its sort needs
     * every row.
     */
    private static EsqlQueryRequest topTenOfBoth() {
        return fetchPhase("FROM " + FAST + "," + SLOW + " | WHERE pause >= 0 | SORT ts DESC | LIMIT 10 | KEEP ts, payload");
    }

    private static EsqlQueryRequest fetchPhase(String query) {
        return syncEsqlQueryRequest(query).pragmas(
            new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_PHASE.getKey(), true).build())
        );
    }

    private static void assertTheWinnersOfTheFastIndex(EsqlQueryResponse response) {
        List<List<Object>> rows = getValuesList(response);
        List<List<Object>> expected = new ArrayList<>();
        for (long ts = 119; ts >= 110; ts--) {
            expected.add(List.of(ts, "payload-" + ts));
        }
        assertThat(rows, equalTo(expected));
        assertFalse(response.isPartial());
    }
}
