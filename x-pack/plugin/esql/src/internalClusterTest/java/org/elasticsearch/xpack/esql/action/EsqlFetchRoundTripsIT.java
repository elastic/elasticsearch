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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.fetch.FetchContextService;
import org.elasticsearch.xpack.esql.fetch.FetchService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The requests a query with the fetch phase sends after the cut of its coordinator, counted per node that receives them:
 * fetch requests, requests that free fetch contexts, and keep-alive requests. A request to the coordinator itself is
 * handed over without the network, but it counts too.
 * <p>
 * Each of the three data nodes holds two single-shard indices. Two nodes hold the winners of the cut, one index per
 * remainder of {@code ts} by four, so every winner index holds some winners of a cut of ten rows. The third node holds
 * older documents, which lose. Every shard sends rows to the coordinator, so every shard keeps a fetch context.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 3)
public class EsqlFetchRoundTripsIT extends AbstractEsqlIntegTestCase {
    private static final String PREFIX = "round_trips_";
    private static final int WINNERS = 20;
    private static final int LOSERS = 20;

    /**
     * Without node-level reduction the data drivers send their rows straight to the coordinator. Every query of a test
     * uses the same choice.
     */
    private final boolean nodeLevelReduction = randomBoolean();

    private String winnerNode;
    private String otherWinnerNode;
    private String loserNode;

    private final Map<String, AtomicInteger> fetches = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> frees = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> keepAlives = new ConcurrentHashMap<>();
    private final Set<ShardSearchContextId> registered = ConcurrentHashMap.newKeySet();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // counts the requests each node receives
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Before
    public void createIndicesAndCountRequests() {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assumeTrue("the fetch_phase pragma needs a snapshot build", canUseQueryPragmas());
        List<String> nodes = new ArrayList<>();
        for (DiscoveryNode node : internalCluster().clusterService().state().nodes().getDataNodes().values()) {
            nodes.add(node.getName());
        }
        Collections.shuffle(nodes, random());
        winnerNode = nodes.get(0);
        otherWinnerNode = nodes.get(1);
        loserNode = nodes.get(2);

        String[] winnerIndices = { "winner_0", "winner_1", "other_winner_2", "other_winner_3" };
        for (int i = 0; i < winnerIndices.length; i++) {
            createIndex(winnerIndices[i], i < 2 ? winnerNode : otherWinnerNode);
        }
        createIndex("loser_0", loserNode);
        createIndex("loser_1", loserNode);
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < WINNERS; i++) {
            long ts = 100 + i;
            bulk.add(prepareIndex(PREFIX + winnerIndices[i % 4]).setSource("ts", ts, "payload", "payload-" + ts));
        }
        for (int i = 0; i < LOSERS; i++) {
            bulk.add(prepareIndex(PREFIX + "loser_" + (i % 2)).setSource("ts", i, "payload", "payload-" + i));
        }
        assertFalse(bulk.get().hasFailures());
        ensureGreen(PREFIX + "*");

        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            MockSearchService mock = (MockSearchService) searchService;
            mock.setOnPutContext(context -> registered.add(context.id()));
            mock.setOnRemoveContext(context -> registered.remove(context.id()));
        }
        for (String node : nodes) {
            MockTransportService transport = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            count(transport, node, FetchService.FETCH_ACTION_NAME, fetches);
            count(transport, node, SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME, frees);
            count(transport, node, FetchContextService.KEEP_ALIVE_ACTION_NAME, keepAlives);
        }
    }

    @After
    public void stopCounting() {
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
     * A {@code TopN} cut of one page sends one fetch request to each node with winners, which frees its contexts once it
     * answered. The coordinator frees the contexts of the losers once the cut ended, with a request for each.
     */
    public void testATopNCut() throws Exception {
        List<List<Object>> rows = runQuery(randomCoordinator(), "FROM " + PREFIX + "* | SORT ts DESC | LIMIT 10 | KEEP ts, payload", 0);

        assertThat(rows, equalTo(newestWinners(10)));
        assertAllContextsFreed();
        assertThat("fetches", counts(fetches), equalTo(Map.of(winnerNode, 1, otherWinnerNode, 1)));
        assertThat("frees", counts(frees), equalTo(Map.of(loserNode, 2)));
        assertThat("keep-alives", counts(keepAlives), anEmptyMap());
    }

    /**
     * The coordinator's {@code TopN} emits its cut in pages of five rows here, and each page holds winners of both nodes.
     * The cut is complete before its first page arrives, so it still sends one fetch request to each node with winners.
     */
    public void testATopNCutOfSeveralPages() throws Exception {
        List<List<Object>> rows = runQuery(randomCoordinator(), "FROM " + PREFIX + "* | SORT ts DESC | LIMIT 10 | KEEP ts, payload", 5);

        assertThat(rows, equalTo(newestWinners(10)));
        assertAllContextsFreed();
        assertThat("fetches", counts(fetches), equalTo(Map.of(winnerNode, 1, otherWinnerNode, 1)));
        assertThat("frees", counts(frees), equalTo(Map.of(loserNode, 2)));
        assertThat("keep-alives", counts(keepAlives), anEmptyMap());
    }

    /**
     * Every winner sits on the coordinator, so the only fetch request stays on the coordinator, and frees its contexts.
     * The node of the losers gets a free for each of its contexts.
     */
    public void testATopNCutWhoseWinnersAllSitOnTheCoordinator() throws Exception {
        String query = "FROM " + PREFIX + "winner_*," + PREFIX + "loser_* | SORT ts DESC | LIMIT 10 | KEEP ts, payload";
        List<List<Object>> rows = runQuery(winnerNode, query, 0);

        List<List<Object>> expected = new ArrayList<>();
        for (long ts = 100 + WINNERS - 1; ts >= 100; ts--) {
            if (ts % 4 < 2) {
                expected.add(List.of(ts, "payload-" + ts));
            }
        }
        assertThat(rows, equalTo(expected));
        assertAllContextsFreed();
        assertThat("fetches", counts(fetches), equalTo(Map.of(winnerNode, 1)));
        assertThat("frees", counts(frees), equalTo(Map.of(loserNode, 2)));
        assertThat("keep-alives", counts(keepAlives), anEmptyMap());
    }

    /**
     * A {@code LIMIT} cut fetches its pages as they arrive, so how many requests a node gets depends on when its pages
     * arrive. The limit is above the number of documents, so every row wins and every context is read. A fetch request
     * that goes out once the input ended frees its contexts, and the coordinator frees the contexts read only before.
     */
    public void testALimitCut() throws Exception {
        String query = "FROM " + PREFIX + "winner_*," + PREFIX + "other_winner_* | LIMIT 100 | KEEP ts, payload";
        List<List<Object>> rows = runQuery(randomCoordinator(), query, 0);

        assertThat(rows, hasSize(WINNERS));
        for (List<Object> row : rows) {
            assertThat(row.get(1), equalTo("payload-" + row.get(0)));
        }
        assertAllContextsFreed();
        Map<String, Integer> fetchCounts = counts(fetches);
        assertThat("fetches", fetchCounts.keySet(), equalTo(Set.of(winnerNode, otherWinnerNode)));
        // at most a page per shard, and a node holds two shards
        for (int fetchesOfNode : fetchCounts.values()) {
            assertThat("fetches of a node", fetchesOfNode, allOf(greaterThanOrEqualTo(1), lessThanOrEqualTo(2)));
        }
        Map<String, Integer> freeCounts = counts(frees);
        assertTrue(
            "frees go to nodes with winners only, but were " + freeCounts,
            Set.of(winnerNode, otherWinnerNode).containsAll(freeCounts.keySet())
        );
        for (int freesOfNode : freeCounts.values()) {
            assertThat("frees of a node", freesOfNode, lessThanOrEqualTo(2));
        }
        assertThat("keep-alives", counts(keepAlives), anEmptyMap());
    }

    private void createIndex(String name, String node) {
        assertAcked(
            indicesAdmin().prepareCreate(PREFIX + name)
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", node))
                .setMapping("ts", "type=long", "payload", "type=keyword")
        );
    }

    private String randomCoordinator() {
        return randomFrom(winnerNode, otherWinnerNode, loserNode);
    }

    /**
     * Runs {@code query} with the fetch phase from {@code coordinator}.
     *
     * @param pageSize the rows of each page, {@code 0} for the default
     */
    private List<List<Object>> runQuery(String coordinator, String query, int pageSize) {
        // one driver per shard keeps the pages of each shard deterministic
        Settings.Builder pragmas = Settings.builder()
            .put(QueryPragmas.FETCH_PHASE.getKey(), true)
            .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1)
            .put(QueryPragmas.DATA_PARTITIONING.getKey(), "shard")
            .put(QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), nodeLevelReduction);
        if (pageSize > 0) {
            pragmas.put(QueryPragmas.PAGE_SIZE.getKey(), pageSize);
        }
        EsqlQueryRequest request = syncEsqlQueryRequest(query).acceptedPragmaRisks(true).pragmas(new QueryPragmas(pragmas.build()));
        try (EsqlQueryResponse response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS)) {
            return getValuesList(response);
        }
    }

    /**
     * Waits until every context the query registered is gone. A free request is counted before it frees its contexts.
     */
    private void assertAllContextsFreed() throws Exception {
        assertBusy(() -> assertThat(registered, empty()));
    }

    private static List<List<Object>> newestWinners(int count) {
        List<List<Object>> rows = new ArrayList<>();
        for (long ts = 100 + WINNERS - 1; rows.size() < count; ts--) {
            rows.add(List.of(ts, "payload-" + ts));
        }
        return rows;
    }

    private static void count(MockTransportService transport, String node, String action, Map<String, AtomicInteger> counts) {
        transport.<TransportRequest>addRequestHandlingBehavior(action, (handler, request, channel, task) -> {
            counts.computeIfAbsent(node, n -> new AtomicInteger()).incrementAndGet();
            handler.messageReceived(request, channel, task);
        });
    }

    private static Map<String, Integer> counts(Map<String, AtomicInteger> counts) {
        Map<String, Integer> snapshot = new HashMap<>();
        counts.forEach((node, count) -> snapshot.put(node, count.get()));
        return snapshot;
    }
}
