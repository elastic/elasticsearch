/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.Build;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.fetch.FetchService;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * The fetch phase of ES|QL on stateless nodes, where the search nodes serve the query phase and the fetch from the reader
 * contexts they open. A search node that leaves between the phases, as a scale-in does, drops the rows of its shards with
 * {@code allow_partial_results} and fails the query without it. The fetch doesn't open a new reader on another node.
 * <p>
 * Each test indexes {@link #KEPT} and {@link #LOST}, whose rows alternate in the order of the sort, with the search shard of
 * each on its own search node. The coordinator is the index node, which holds no search shard.
 */
public class StatelessEsqlFetchPhaseIT extends AbstractStatelessPluginIntegTestCase {
    private static final String LOCAL = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
    private static final String KEPT = "fetch_kept";
    private static final String LOST = "fetch_lost";
    private static final int DOCS_PER_INDEX = 20;
    private static final int LIMIT = 10;
    private static final String QUERY = "FROM " + KEPT + "," + LOST + " | SORT ts DESC | LIMIT " + LIMIT + " | KEEP ts, payload";

    private record Nodes(String coordinator, String keptSearchNode, String lostSearchNode) {}

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // ES|QL reads the encryption service of its data sources as it starts, so the service comes first
        plugins.add(EncryptionPlugin.class);
        plugins.add(EsqlPlugin.class);
        return plugins;
    }

    @Before
    public void checkTheFetchPhaseIsAvailable() {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assumeTrue("the fetch_phase pragma needs a snapshot build", Build.current().isSnapshot());
    }

    /**
     * A search node that leaves after the query phase, before it serves the fetch, drops the rows of its shards. The rows of
     * the other shards come back in the order of the sort, and the response is partial.
     */
    public void testASearchNodeThatLeavesBetweenThePhasesDropsTheRowsOfItsShards() throws Exception {
        Nodes nodes = startNodesAndCreateIndices();
        ActionFuture<EsqlQueryResponse> query = queryWhileTheLostNodeLeaves(nodes, true);
        try (EsqlQueryResponse response = query.actionGet(30, TimeUnit.SECONDS)) {
            assertThat(rows(response), equalTo(rowsOfKept()));
            assertTrue(response.isPartial());
            EsqlExecutionInfo.Cluster local = response.getExecutionInfo().getCluster(LOCAL);
            assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertThat(local.getFailures(), not(empty()));
            for (ShardSearchFailure failure : local.getFailures()) {
                assertThat(failure.index(), equalTo(LOST));
                assertThat(failure.getCause(), instanceOf(ConnectTransportException.class));
            }
        }
        assertNoContextLeftOn(nodes.keptSearchNode());
    }

    /**
     * Without {@code allow_partial_results}, a search node that leaves between the phases fails the query.
     */
    public void testWithoutPartialResultsASearchNodeThatLeavesBetweenThePhasesFailsTheQuery() throws Exception {
        Nodes nodes = startNodesAndCreateIndices();
        ActionFuture<EsqlQueryResponse> query = queryWhileTheLostNodeLeaves(nodes, false);
        Exception e = expectThrows(Exception.class, () -> query.actionGet(30, TimeUnit.SECONDS).close());
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(ConnectTransportException.class));
        assertNoContextLeftOn(nodes.keptSearchNode());
    }

    /**
     * A search shard that relocates between the phases leaves the reader context on the node it left, which still serves
     * the fetch from the commit the query phase read. Documents rewritten meanwhile keep the values of that commit.
     */
    public void testASearchShardThatRelocatesBetweenThePhasesStillFetches() throws Exception {
        Nodes nodes = startNodesAndCreateIndices();
        String target = startSearchNode();
        CountDownLatch queryPhaseAnswered = queryPhaseAnswered(nodes.coordinator(), nodes.lostSearchNode());
        CountDownLatch fetchSent = new CountDownLatch(1);
        CountDownLatch relocated = new CountDownLatch(1);
        holdTheFetchTo(nodes.coordinator(), nodes.lostSearchNode(), fetchSent, relocated);
        ActionFuture<EsqlQueryResponse> query = client(nodes.coordinator()).execute(EsqlQueryAction.INSTANCE, request(true));
        try {
            safeAwait(queryPhaseAnswered);
            safeAwait(fetchSent);
            updateIndexSettings(onlyOn(nodes.coordinator(), target), LOST);
            assertBusy(() -> assertThat(searchNodeOf(LOST), equalTo(target)));
            ensureGreen(LOST);
            indexDocuments(LOST, 1, "rewritten-");
            flush(LOST);
        } finally {
            relocated.countDown();
        }
        try (EsqlQueryResponse response = query.actionGet(30, TimeUnit.SECONDS)) {
            assertThat(rows(response), equalTo(allWinners()));
            assertFalse(response.isPartial());
        }
        assertNoContextLeftOn(nodes.keptSearchNode(), nodes.lostSearchNode(), target);
    }

    /**
     * The query freed the contexts it opened on {@code searchNodes}, also on a node its shard left.
     */
    private static void assertNoContextLeftOn(String... searchNodes) throws Exception {
        for (String node : searchNodes) {
            SearchService searchService = internalCluster().getInstance(SearchService.class, node);
            assertBusy(() -> assertThat("open contexts on " + node, searchService.getActiveContexts(), equalTo(0)));
        }
    }

    /**
     * Starts the index node, which coordinates, and a search node for each index, and indexes them.
     */
    private Nodes startNodesAndCreateIndices() {
        String coordinator = startMasterAndIndexNode();
        String keptSearchNode = startSearchNode();
        String lostSearchNode = startSearchNode();
        createIndex(KEPT, onlyOn(coordinator, keptSearchNode), 0);
        createIndex(LOST, onlyOn(coordinator, lostSearchNode), 1);
        ensureGreen(KEPT, LOST);
        assertThat(searchNodeOf(KEPT), equalTo(keptSearchNode));
        assertThat(searchNodeOf(LOST), equalTo(lostSearchNode));
        return new Nodes(coordinator, keptSearchNode, lostSearchNode);
    }

    /**
     * Keeps the shards of an index on {@code indexNode} and its search shards on {@code searchNode}, also once more search
     * nodes join.
     */
    private static Settings.Builder onlyOn(String indexNode, String searchNode) {
        return Settings.builder().put("index.routing.allocation.include._name", indexNode + "," + searchNode);
    }

    /**
     * One shard and one search shard, with {@link #DOCS_PER_INDEX} documents whose {@code ts} start at {@code firstTs} in
     * steps of two.
     */
    private void createIndex(String index, Settings.Builder allocation, long firstTs) {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(allocation.build())
            ).setMapping("ts", "type=long", "payload", "type=keyword")
        );
        indexDocuments(index, firstTs, "payload-");
    }

    /**
     * Indexes or rewrites the {@link #DOCS_PER_INDEX} documents of {@code index}, whose ids are their {@code ts}.
     */
    private void indexDocuments(String index, long firstTs, String payloadPrefix) {
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < DOCS_PER_INDEX; i++) {
            long ts = firstTs + 2L * i;
            bulk.add(prepareIndex(index).setId(Long.toString(ts)).setSource("ts", ts, "payload", payloadPrefix + ts));
        }
        assertFalse(bulk.get().hasFailures());
    }

    /**
     * Runs {@link #QUERY} and stops the search node of {@link #LOST} once it answered the query phase, while the coordinator
     * holds the fetch it sends it.
     */
    private ActionFuture<EsqlQueryResponse> queryWhileTheLostNodeLeaves(Nodes nodes, boolean allowPartialResults) throws Exception {
        CountDownLatch queryPhaseAnswered = queryPhaseAnswered(nodes.coordinator(), nodes.lostSearchNode());
        CountDownLatch fetchSent = new CountDownLatch(1);
        CountDownLatch nodeStopped = new CountDownLatch(1);
        holdTheFetchTo(nodes.coordinator(), nodes.lostSearchNode(), fetchSent, nodeStopped);
        ActionFuture<EsqlQueryResponse> query = client(nodes.coordinator()).execute(EsqlQueryAction.INSTANCE, request(allowPartialResults));
        try {
            safeAwait(queryPhaseAnswered);
            safeAwait(fetchSent);
            internalCluster().stopNode(nodes.lostSearchNode());
        } finally {
            nodeStopped.countDown();
        }
        return query;
    }

    /**
     * Holds the fetch that {@code coordinator} sends to {@code node} until {@code release} counts down.
     */
    private static void holdTheFetchTo(String coordinator, String node, CountDownLatch fetchSent, CountDownLatch release) {
        mockTransport(coordinator).addSendBehavior(
            internalCluster().getInstance(TransportService.class, node),
            (connection, requestId, action, request, options) -> {
                if (action.equals(FetchService.FETCH_ACTION_NAME)) {
                    fetchSent.countDown();
                    safeAwait(release);
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );
    }

    /**
     * Counts down once {@code coordinator} receives the answer of {@code dataNode} to the query phase.
     */
    private static CountDownLatch queryPhaseAnswered(String coordinator, String dataNode) {
        CountDownLatch answered = new CountDownLatch(1);
        mockTransport(coordinator).addMessageListener(new TransportMessageListener() {
            @Override
            @SuppressWarnings("rawtypes")
            public void onResponseReceived(long requestId, Transport.ResponseContext context, int networkMessageSize) {
                if (context != null
                    && context.action().equals(ComputeService.DATA_ACTION_NAME)
                    && context.connection().getNode().getName().equals(dataNode)) {
                    answered.countDown();
                }
            }
        });
        return answered;
    }

    private static List<List<Object>> rows(EsqlQueryResponse response) {
        List<List<Object>> rows = new ArrayList<>();
        for (Iterable<Object> row : response.rows()) {
            List<Object> values = new ArrayList<>();
            row.forEach(values::add);
            rows.add(values);
        }
        return rows;
    }

    private String searchNodeOf(String index) {
        var routingTable = clusterService().state().routingTable().index(index).shard(0);
        var searchShards = routingTable.assignedUnpromotableShards();
        assertThat(searchShards.size(), equalTo(1));
        return clusterService().state().nodes().get(searchShards.getFirst().currentNodeId()).getName();
    }

    private static EsqlQueryRequest request(boolean allowPartialResults) {
        return new EsqlQueryRequest().query(QUERY)
            .pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_PHASE.getKey(), true).build()))
            .allowPartialResults(allowPartialResults);
    }

    /**
     * The rows of {@link #QUERY}: the {@link #LIMIT} highest {@code ts} of both indices.
     */
    private static List<List<Object>> allWinners() {
        long highest = 2L * DOCS_PER_INDEX - 1;
        return LongStream.iterate(highest, ts -> ts - 1).limit(LIMIT).mapToObj(ts -> List.<Object>of(ts, "payload-" + ts)).toList();
    }

    /**
     * The rows of {@link #QUERY} that come from {@link #KEPT}: the even {@code ts} among the winners.
     */
    private static List<List<Object>> rowsOfKept() {
        return allWinners().stream().filter(row -> (Long) row.get(0) % 2 == 0).toList();
    }

    private static MockTransportService mockTransport(String node) {
        return (MockTransportService) internalCluster().getInstance(TransportService.class, node);
    }
}
