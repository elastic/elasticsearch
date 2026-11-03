/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.fetch.FetchContextService;
import org.elasticsearch.xpack.esql.fetch.FetchRequest;
import org.elasticsearch.xpack.esql.fetch.FetchResponse;
import org.elasticsearch.xpack.esql.fetch.FetchService;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Fetches that a node failure or a circuit breaker interrupts.
 * <p>
 * A node that leaves while it serves a fetch is a shard failure, like a node that leaves during the query phase. With
 * {@code allow_partial_results} the rows of its shards drop and the response is partial. Without it the query fails.
 * <p>
 * A request breaker that trips while a data node loads the fetched columns, or while the coordinator reads them, fails the
 * query with 429 either way, like a trip in the query phase, and the query releases every context and every byte it held.
 * The breaker tests query an index of big payloads and an index of small ones, whose rows alternate. One node gets a
 * request breaker that fits the sort keys of the query phase and the small payloads, but not the big ones. The rows of
 * the small index would come back if the query went on without the shard that tripped.
 * <p>
 * Each test starts the nodes it needs.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class EsqlFetchDisruptionIT extends AbstractEsqlIntegTestCase {
    private static final String LOCAL = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
    /** Documents on a node that stays. */
    private static final String KEPT = "disruption_kept";
    /** Documents on the node that leaves. */
    private static final String LOST = "disruption_lost";
    private static final int DOCS_PER_INDEX = 20;
    private static final int LIMIT = 10;
    private static final String QUERY = "FROM " + KEPT + "," + LOST + " | SORT ts DESC | LIMIT " + LIMIT + " | KEEP ts, payload";
    /** {@link #KEPT} has the even {@code ts} from 0, so these odd ones alternate with them and both indices have winners. */
    private static final long LOST_TS_AMONG_THE_WINNERS = 1;
    /** Far below every {@code ts} of {@link #KEPT}, so none of {@link #LOST} wins. */
    private static final long LOST_TS_BELOW_THE_WINNERS = -1000;

    /** Documents with even {@code ts} and payloads that only {@code _source} holds. */
    private static final String BIG = "disruption_big";
    /** Documents with odd {@code ts} and small payloads. */
    private static final String SMALL = "disruption_small";
    private static final int BREAKER_DOCS_PER_INDEX = 8;
    private static final String BREAKER_QUERY = "FROM " + BIG + "," + SMALL + " | SORT ts DESC | LIMIT " + LIMIT + " | KEEP ts, payload";
    /** The payloads of the winners of {@link #BIG} together are a few times {@link #SMALL_REQUEST_BREAKER}. */
    private static final int BIG_PAYLOAD_CHARS = 200 * 1024;
    /** Room for the sort keys and the document ids of the query phase, but not for the payloads of the winners. */
    private static final String SMALL_REQUEST_BREAKER = "300kb";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Before
    public void checkTheFetchPhaseIsAvailable() {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assumeTrue("the fetch_phase pragma needs a snapshot build", canUseQueryPragmas());
    }

    /**
     * A data node that leaves after it received a fetch, before it answers, drops the rows of its shards. The rows of the
     * other shards come back in the order of the sort, and the response is partial, with the shards of the node listed as
     * failed.
     */
    public void testANodeThatLeavesWhileItServesAFetchDropsTheRowsOfItsShards() throws Exception {
        Nodes nodes = startNodesAndCreateIndices(LOST_TS_AMONG_THE_WINNERS);
        CountDownLatch fetchArrived = neverAnswerFetches(nodes.lost());
        ActionFuture<EsqlQueryResponse> query = client(nodes.coordinator()).execute(EsqlQueryAction.INSTANCE, request(QUERY, true));
        safeAwait(fetchArrived);
        internalCluster().stopNode(nodes.lost());
        try (EsqlQueryResponse response = query.actionGet(30, TimeUnit.SECONDS)) {
            assertThat(getValuesList(response), equalTo(rowsOfKeptAmongTheWinners(LOST_TS_AMONG_THE_WINNERS)));
            assertTrue(response.isPartial());
            EsqlExecutionInfo.Cluster local = response.getExecutionInfo().getCluster(LOCAL);
            assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertThat(local.getFailures(), not(empty()));
            for (ShardSearchFailure failure : local.getFailures()) {
                assertThat(failure.index(), equalTo(LOST));
                assertThat(failure.getCause(), instanceOf(ConnectTransportException.class));
            }
        }
    }

    /**
     * Without {@code allow_partial_results}, a data node that leaves while it serves a fetch fails the query.
     */
    public void testWithoutPartialResultsANodeThatLeavesWhileItServesAFetchFailsTheQuery() throws Exception {
        Nodes nodes = startNodesAndCreateIndices(LOST_TS_AMONG_THE_WINNERS);
        CountDownLatch fetchArrived = neverAnswerFetches(nodes.lost());
        ActionFuture<EsqlQueryResponse> query = client(nodes.coordinator()).execute(EsqlQueryAction.INSTANCE, request(QUERY, false));
        safeAwait(fetchArrived);
        internalCluster().stopNode(nodes.lost());
        Exception e = expectThrows(Exception.class, () -> query.actionGet(30, TimeUnit.SECONDS).close());
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(ConnectTransportException.class));
    }

    /**
     * A data node whose rows all lose the cut can leave before the fetch, even without {@code allow_partial_results}. The
     * coordinator sends it no fetch, and the free of its contexts finds the node gone, so the response is complete.
     */
    public void testANodeWithoutWinnersCanLeaveBeforeTheFetch() throws Exception {
        Nodes nodes = startNodesAndCreateIndices(LOST_TS_BELOW_THE_WINNERS);
        CountDownLatch queryPhaseAnswered = queryPhaseAnswered(nodes.coordinator(), nodes.lost());
        CountDownLatch freeSent = new CountDownLatch(1);
        CountDownLatch nodeStopped = new CountDownLatch(1);
        List<String> actionsSentToTheLostNode = new CopyOnWriteArrayList<>();
        mockTransport(nodes.coordinator()).addSendBehavior(
            internalCluster().getInstance(TransportService.class, nodes.lost()),
            (connection, requestId, action, request, options) -> {
                if (action.startsWith(FetchService.FETCH_ACTION_NAME)) {
                    actionsSentToTheLostNode.add(action);
                }
                if (action.equals(FetchContextService.FREE_ACTION_NAME)) {
                    freeSent.countDown();
                    safeAwait(nodeStopped);
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );
        ActionFuture<EsqlQueryResponse> query = client(nodes.coordinator()).execute(EsqlQueryAction.INSTANCE, request(QUERY, false));
        try {
            safeAwait(queryPhaseAnswered);
            safeAwait(freeSent);
            internalCluster().stopNode(nodes.lost());
        } finally {
            nodeStopped.countDown();
        }
        try (EsqlQueryResponse response = query.actionGet(30, TimeUnit.SECONDS)) {
            assertThat(getValuesList(response), equalTo(rowsOfKeptAmongTheWinners(LOST_TS_BELOW_THE_WINNERS)));
            assertFalse(response.isPartial());
            assertThat(response.getExecutionInfo().getCluster(LOCAL).getFailures(), empty());
        }
        assertThat(actionsSentToTheLostNode, contains(FetchContextService.FREE_ACTION_NAME));
    }

    /**
     * A data node whose request breaker trips while it loads the fetched columns fails the query with 429, like a trip in
     * the query phase. The data node reports the trip as the failure of the shard in its response, and
     * {@code allow_partial_results} doesn't make the query go on without the rows of that shard.
     */
    public void testABreakerThatTripsWhileADataNodeLoadsTheFetchFailsTheQuery() {
        internalCluster().startMasterOnlyNode();
        String tripping = internalCluster().startDataOnlyNode(smallRequestBreaker());
        String coordinator = internalCluster().startDataOnlyNode();
        createBreakerIndices(tripping, coordinator);
        List<List<Exception>> fetchFailures = recordFetchFailures(tripping);
        for (boolean allowPartialResults : new boolean[] { true, false }) {
            long tripsBefore = requestBreakerTrips(tripping);
            fetchFailures.clear();
            assertTripped(expectThrows(Exception.class, () -> runBreakerQuery(coordinator, allowPartialResults)));
            assertThat("the data node tripped once per query", requestBreakerTrips(tripping), equalTo(tripsBefore + 1));
            assertThat(fetchFailures, hasSize(1));
            assertThat(
                "the data node reports the shard as tripped",
                fetchFailures.get(0),
                contains(instanceOf(CircuitBreakingException.class))
            );
        }
        ensureBlocksReleased();
    }

    /**
     * A coordinator whose request breaker trips while it reads the fetched columns of one node fails the query with 429.
     * That node answered the fetch, and {@code allow_partial_results} doesn't make the query go on without its rows.
     */
    public void testABreakerThatTripsWhileTheCoordinatorReadsTheFetchFailsTheQuery() {
        internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        String coordinator = internalCluster().startCoordinatingOnlyNode(smallRequestBreaker());
        createBreakerIndices(dataNodes.get(0), dataNodes.get(1));
        List<List<Exception>> fetchFailures = recordFetchFailures(dataNodes.get(0));
        for (boolean allowPartialResults : new boolean[] { true, false }) {
            long tripsBefore = requestBreakerTrips(coordinator);
            fetchFailures.clear();
            assertTripped(expectThrows(Exception.class, () -> runBreakerQuery(coordinator, allowPartialResults)));
            assertThat("the coordinator tripped once per query", requestBreakerTrips(coordinator), equalTo(tripsBefore + 1));
            assertThat(fetchFailures, hasSize(1));
            assertThat("the data node loaded every row", fetchFailures.get(0), empty());
        }
        ensureBlocksReleased();
    }

    /**
     * Only the big payloads exceed the small breaker: without them the same query runs on nodes that all have it.
     */
    public void testTheQueryPhaseFitsTheSmallBreaker() {
        internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2, smallRequestBreaker());
        String coordinator = internalCluster().startCoordinatingOnlyNode(smallRequestBreaker());
        createBreakerIndices(dataNodes.get(0), dataNodes.get(1));
        String sortKeysOnly = "FROM " + BIG + "," + SMALL + " | SORT ts DESC | LIMIT " + LIMIT + " | KEEP ts";
        try (
            EsqlQueryResponse response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(sortKeysOnly, false))
                .actionGet(30, TimeUnit.SECONDS)
        ) {
            List<List<Object>> expected = LongStream.iterate(2L * BREAKER_DOCS_PER_INDEX - 1, ts -> ts - 1)
                .limit(LIMIT)
                .mapToObj(ts -> List.<Object>of(ts))
                .toList();
            assertThat(getValuesList(response), equalTo(expected));
        }
    }

    private record Nodes(String coordinator, String kept, String lost) {}

    /**
     * Starts a master, a data node for {@link #KEPT}, a data node for {@link #LOST} and a data node without shards of
     * either that coordinates the queries.
     */
    private Nodes startNodesAndCreateIndices(long firstTsOfLost) {
        internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
        Nodes nodes = new Nodes(dataNodes.get(0), dataNodes.get(1), dataNodes.get(2));
        createIndex(KEPT, nodes.kept(), 0);
        createIndex(LOST, nodes.lost(), firstTsOfLost);
        ensureGreen(KEPT, LOST);
        return nodes;
    }

    /**
     * {@link #DOCS_PER_INDEX} documents on {@code node}, with {@code ts} from {@code firstTs} in steps of two.
     */
    private void createIndex(String index, String node, long firstTs) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(between(1, 3), 0).put("index.routing.allocation.require._name", node))
                .setMapping("ts", "type=long", "payload", "type=keyword")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (long ts : tsOf(firstTs)) {
            bulk.add(prepareIndex(index).setSource("ts", ts, "payload", "payload-" + ts));
        }
        assertFalse(bulk.get().hasFailures());
    }

    private static List<Long> tsOf(long firstTs) {
        return LongStream.range(0, DOCS_PER_INDEX).map(i -> firstTs + 2 * i).boxed().toList();
    }

    /**
     * The rows of {@link #QUERY} that come from {@link #KEPT}, when the {@code ts} of {@link #LOST} start at
     * {@code firstTsOfLost}.
     */
    private static List<List<Object>> rowsOfKeptAmongTheWinners(long firstTsOfLost) {
        List<Long> kept = tsOf(0);
        return Stream.concat(kept.stream(), tsOf(firstTsOfLost).stream())
            .sorted(Comparator.reverseOrder())
            .limit(LIMIT)
            .filter(kept::contains)
            .map(ts -> List.<Object>of(ts, "payload-" + ts))
            .toList();
    }

    /**
     * {@link #BIG} on one shard on {@code bigNode}, with payloads of {@link #BIG_PAYLOAD_CHARS} that only {@code _source}
     * holds, so only loading them costs memory. {@link #SMALL} on one shard on {@code smallNode}.
     */
    private void createBreakerIndices(String bigNode, String smallNode) {
        assertAcked(
            indicesAdmin().prepareCreate(BIG)
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", bigNode))
                .setMapping("ts", "type=long", "payload", "type=keyword,index=false,doc_values=false")
        );
        assertAcked(
            indicesAdmin().prepareCreate(SMALL)
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", smallNode))
                .setMapping("ts", "type=long", "payload", "type=keyword")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < BREAKER_DOCS_PER_INDEX; i++) {
            bulk.add(prepareIndex(BIG).setSource("ts", 2 * i, "payload", randomAlphaOfLength(BIG_PAYLOAD_CHARS)));
            bulk.add(prepareIndex(SMALL).setSource("ts", 2 * i + 1, "payload", "payload-" + (2 * i + 1)));
        }
        assertFalse(bulk.get().hasFailures());
        ensureGreen(BIG, SMALL);
    }

    private static Settings smallRequestBreaker() {
        return Settings.builder()
            // the test cluster gives some nodes a request breaker that never trips
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), CircuitBreaker.Type.MEMORY)
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), SMALL_REQUEST_BREAKER)
            .build();
    }

    private static void runBreakerQuery(String coordinator, boolean allowPartialResults) {
        client(coordinator).execute(EsqlQueryAction.INSTANCE, request(BREAKER_QUERY, allowPartialResults))
            .actionGet(30, TimeUnit.SECONDS)
            .close();
    }

    private static void assertTripped(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        assertThat(cause, instanceOf(CircuitBreakingException.class));
        assertThat(ExceptionsHelper.status(cause), equalTo(RestStatus.TOO_MANY_REQUESTS));
    }

    private static long requestBreakerTrips(String node) {
        return internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST).getTrippedCount();
    }

    /**
     * Swallows every fetch that {@code node} receives, as if the node stopped answering, and counts down once one arrives.
     */
    private static CountDownLatch neverAnswerFetches(String node) {
        CountDownLatch fetchArrived = new CountDownLatch(1);
        mockTransport(node).<FetchRequest>addRequestHandlingBehavior(
            FetchService.FETCH_ACTION_NAME,
            (handler, request, channel, task) -> fetchArrived.countDown()
        );
        return fetchArrived;
    }

    /**
     * Records the failures that {@code node} reports in its answer to each fetch: the shards that failed in its response, or
     * the exception it sends back instead of one.
     */
    private static List<List<Exception>> recordFetchFailures(String node) {
        List<List<Exception>> answers = new CopyOnWriteArrayList<>();
        mockTransport(node).<FetchRequest>addRequestHandlingBehavior(
            FetchService.FETCH_ACTION_NAME,
            (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                @Override
                public String getProfileName() {
                    return channel.getProfileName();
                }

                @Override
                public void sendResponse(TransportResponse response) {
                    List<Exception> failures = new ArrayList<>();
                    for (FetchResponse.ShardResult shard : ((FetchResponse) response).shardResults()) {
                        if (shard.failure() != null) {
                            failures.add(shard.failure());
                        }
                    }
                    answers.add(failures);
                    channel.sendResponse(response);
                }

                @Override
                public void sendResponse(Exception exception) {
                    answers.add(List.of(exception));
                    channel.sendResponse(exception);
                }

                @Override
                public TransportVersion getVersion() {
                    return channel.getVersion();
                }
            }, task)
        );
        return answers;
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

    private static EsqlQueryRequest request(String query, boolean allowPartialResults) {
        return syncEsqlQueryRequest(query).pragmas(fetchPhase()).allowPartialResults(allowPartialResults);
    }

    private static QueryPragmas fetchPhase() {
        return new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_PHASE.getKey(), true).build());
    }

    private static MockTransportService mockTransport(String node) {
        return (MockTransportService) internalCluster().getInstance(TransportService.class, node);
    }
}
