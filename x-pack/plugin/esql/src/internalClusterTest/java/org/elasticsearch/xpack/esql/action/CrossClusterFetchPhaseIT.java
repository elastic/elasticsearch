/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.elasticsearch.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.fetch.FetchOperator;
import org.elasticsearch.xpack.esql.fetch.FetchService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * The fetch phase over remote clusters: the data nodes of each remote cluster keep their reader contexts for the querying
 * cluster, the coordinator of the querying cluster cuts the rows of every cluster, and fetches the other columns of the
 * winners straight from the remote data nodes that hold them, through a gateway node of their cluster where needed. Every
 * query returns the rows the same query returns without the fetch phase, and leaves no context open on any cluster.
 * <p>
 * The documents of the clusters interleave by {@code ts}, so the cut of every query keeps rows of every cluster it reads.
 */
public class CrossClusterFetchPhaseIT extends AbstractCrossClusterTestCase {
    private static final String INDEX = "fetch_ccs";
    /**
     * The index of a node that a test starts and stops between the phases.
     */
    private static final String LOST_INDEX = "fetch_ccs_lost";
    private static final String LOST_PREFIX = "lost-";
    private static final int DOCS_PER_CLUSTER = 30;
    private static final List<String> CLUSTERS = List.of(LOCAL_CLUSTER, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);

    /**
     * The fetch requests the nodes of each cluster served.
     */
    private final Map<String, AtomicInteger> fetchRequests = new ConcurrentHashMap<>();
    /**
     * The fetch requests that a gateway node of each cluster forwarded to another node of its cluster, or to itself.
     */
    private final Map<String, AtomicInteger> forwardedFetchRequests = new ConcurrentHashMap<>();

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false, REMOTE_CLUSTER_2, true);
    }

    @Before
    public void createIndicesAndCountFetches() {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assumeTrue("the fetch_phase pragma needs a snapshot build", AbstractEsqlIntegTestCase.canUseQueryPragmas());
        for (int c = 0; c < CLUSTERS.size(); c++) {
            String clusterAlias = CLUSTERS.get(c);
            Client client = client(clusterAlias);
            int dataNodes = cluster(clusterAlias).numDataNodes();
            // two shards per data node, so every data node holds documents
            assertAcked(
                client.admin()
                    .indices()
                    .prepareCreate(INDEX)
                    .setSettings(Settings.builder().put("index.number_of_shards", dataNodes * 2).put("index.number_of_replicas", 0))
                    .setMapping("ts", "type=long", "a", "type=keyword", "b", "type=long")
            );
            BulkRequestBuilder bulk = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = 0; i < DOCS_PER_CLUSTER; i++) {
                bulk.add(
                    client.prepareIndex(INDEX)
                        .setSource(Map.of("ts", i * CLUSTERS.size() + c, "a", prefix(clusterAlias) + i, "b", i * 10L + c))
                );
            }
            assertFalse(bulk.get().hasFailures());
            fetchRequests.put(clusterAlias, new AtomicInteger());
            forwardedFetchRequests.put(clusterAlias, new AtomicInteger());
            for (TransportService transportService : cluster(clusterAlias).getInstances(TransportService.class)) {
                countFetches(clusterAlias, (MockTransportService) transportService);
            }
        }
    }

    private void countFetches(String clusterAlias, MockTransportService transportService) {
        transportService.<TransportRequest>addRequestHandlingBehavior(FetchService.FETCH_ACTION_NAME, (handler, request, channel, task) -> {
            fetchRequests.get(clusterAlias).incrementAndGet();
            handler.messageReceived(request, channel, task);
        });
        transportService.<TransportRequest>addRequestHandlingBehavior(
            TransportActionProxy.getProxyAction(FetchService.FETCH_ACTION_NAME),
            (handler, request, channel, task) -> {
                forwardedFetchRequests.get(clusterAlias).incrementAndGet();
                handler.messageReceived(request, channel, task);
            }
        );
    }

    @After
    public void deleteIndicesAndCheckNoContextIsLeft() throws Exception {
        for (String clusterAlias : CLUSTERS) {
            for (TransportService transportService : cluster(clusterAlias).getInstances(TransportService.class)) {
                ((MockTransportService) transportService).clearAllRules();
            }
        }
        for (String clusterAlias : CLUSTERS) {
            InternalTestCluster cluster = cluster(clusterAlias);
            assertBusy(() -> {
                for (String node : cluster.getNodeNames()) {
                    int open = cluster.getInstance(SearchService.class, node).getActiveContexts();
                    assertThat("open contexts on [" + node + "] of [" + clusterAlias + "]", open, equalTo(0));
                }
            }, 30, TimeUnit.SECONDS);
        }
        for (String clusterAlias : CLUSTERS) {
            // the clusters serve every test of the class, and a test that was skipped created no index
            assertAcked(
                client(clusterAlias).admin()
                    .indices()
                    .prepareDelete(INDEX, LOST_INDEX)
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            );
        }
    }

    /**
     * A cut over the local cluster and a remote one returns the rows of both, and fetches the rows of each from its own
     * data nodes.
     */
    public void testFetchesTheWinnersOfTheLocalAndARemoteClusterFromTheirNodes() {
        String query = "FROM " + INDEX + "," + REMOTE_CLUSTER_1 + ":" + INDEX + " | SORT ts DESC | LIMIT 12 | KEEP ts, a, b";

        List<List<Object>> fetched = assertSameRowsAsTheEagerQuery(query, 12);

        assertThat(fetched, hasSize(12));
        assertThat(rowsOf(fetched, LOCAL_CLUSTER), greaterThan(0));
        assertThat(rowsOf(fetched, REMOTE_CLUSTER_1), greaterThan(0));
        assertThat(fetchRequests.get(LOCAL_CLUSTER).get(), greaterThan(0));
        assertThat(fetchRequests.get(REMOTE_CLUSTER_1).get(), greaterThan(0));
        assertThat("a cluster the query doesn't read", fetchRequests.get(REMOTE_CLUSTER_2).get(), equalTo(0));
    }

    /**
     * A query that reads remote clusters only has no local data nodes, and fetches from the remote ones alone.
     */
    public void testAQueryOfRemoteClustersOnlyFetchesFromThem() {
        String query = "FROM "
            + REMOTE_CLUSTER_1
            + ":"
            + INDEX
            + ","
            + REMOTE_CLUSTER_2
            + ":"
            + INDEX
            + " | SORT ts DESC | LIMIT 8 | KEEP ts, a";

        List<List<Object>> fetched = assertSameRowsAsTheEagerQuery(query, 8);

        assertThat(rowsOf(fetched, REMOTE_CLUSTER_1), greaterThan(0));
        assertThat(rowsOf(fetched, REMOTE_CLUSTER_2), greaterThan(0));
        assertThat(fetchRequests.get(LOCAL_CLUSTER).get(), equalTo(0));
        assertThat(fetchRequests.get(REMOTE_CLUSTER_1).get(), greaterThan(0));
        assertThat(fetchRequests.get(REMOTE_CLUSTER_2).get(), greaterThan(0));
    }

    /**
     * A {@code LIMIT} cut fetches its pages as they arrive, possibly before a remote cluster answered its request.
     */
    public void testALimitFetchesTheRowsOfEveryCluster() {
        String query = "FROM " + INDEX + ",*:" + INDEX + " | LIMIT 1000 | KEEP a, b";
        int rows = DOCS_PER_CLUSTER * CLUSTERS.size();

        List<List<Object>> eager = sortedByA(rows(query, false));
        List<List<Object>> fetched = sortedByA(rows(query, true));

        assertThat(fetched, equalTo(eager));
        assertThat(fetched, hasSize(rows));
        for (String clusterAlias : CLUSTERS) {
            assertThat(fetchRequests.get(clusterAlias).get(), greaterThan(0));
        }
    }

    /**
     * In proxy mode the coordinator reaches the remote cluster through one address, and a gateway node forwards the
     * fetches of the documents of the other nodes.
     */
    public void testAGatewayForwardsTheFetchesInProxyMode() throws Exception {
        String remoteNode = randomFrom(cluster(REMOTE_CLUSTER_1).getNodeNames());
        String proxyAddress = cluster(REMOTE_CLUSTER_1).getInstance(TransportService.class, remoteNode)
            .boundAddress()
            .publishAddress()
            .toString();
        updateRemoteCluster(
            REMOTE_CLUSTER_1,
            "proxy",
            Settings.builder()
                .putNull("cluster.remote." + REMOTE_CLUSTER_1 + ".seeds")
                .putNull("cluster.remote." + REMOTE_CLUSTER_1 + ".node_connections")
                .put("cluster.remote." + REMOTE_CLUSTER_1 + ".proxy_address", proxyAddress)
        );

        assertTheGatewayForwardsTheFetchesOfTheOtherNodes();
    }

    /**
     * In sniff mode the coordinator connects to a few nodes of the remote cluster, here one, and a gateway node forwards
     * the fetches of the documents of the other nodes.
     */
    public void testAGatewayForwardsTheFetchesInSniffModeWithOneConnection() throws Exception {
        String seed = cluster(REMOTE_CLUSTER_1).getInstance(TransportService.class, randomFrom(cluster(REMOTE_CLUSTER_1).getNodeNames()))
            .boundAddress()
            .publishAddress()
            .toString();
        updateRemoteCluster(
            REMOTE_CLUSTER_1,
            "sniff",
            Settings.builder()
                .putNull("cluster.remote." + REMOTE_CLUSTER_1 + ".proxy_address")
                .put("cluster.remote." + REMOTE_CLUSTER_1 + ".seeds", seed)
                .put("cluster.remote." + REMOTE_CLUSTER_1 + ".node_connections", 1)
        );

        assertTheGatewayForwardsTheFetchesOfTheOtherNodes();
    }

    /**
     * A remote cluster that the query skips when it fails loses only its own rows when its fetch fails, and reports its
     * shards as failed. The query doesn't allow partial results otherwise.
     */
    public void testAFailedFetchOfASkippedClusterDropsItsRows() {
        String query = "FROM " + INDEX + "," + REMOTE_CLUSTER_2 + ":" + INDEX + " | SORT ts DESC | LIMIT 12 | KEEP ts, a, b";
        List<List<Object>> eager = rows(query, false);
        refuseFetches(REMOTE_CLUSTER_2);

        try (EsqlQueryResponse response = run(query, true, false)) {
            List<List<Object>> fetched = EsqlTestUtils.getValuesList(response);
            List<List<Object>> localRows = eager.stream().filter(row -> ((String) row.get(1)).startsWith(prefix(LOCAL_CLUSTER))).toList();
            assertThat(fetched, equalTo(localRows));
            assertTrue(response.isPartial());
            EsqlExecutionInfo.Cluster remote = response.getExecutionInfo().getCluster(REMOTE_CLUSTER_2);
            assertThat(remote.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertThat(remote.getFailedShards(), greaterThan(0));
            assertThat(remote.getFailures().getFirst().getCause().getMessage(), containsString("fetch refused"));
            EsqlExecutionInfo.Cluster local = response.getExecutionInfo().getCluster(LOCAL_CLUSTER);
            assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        }
    }

    /**
     * A remote cluster that the query doesn't skip when it fails fails the query when its fetch fails, unless the query
     * allows partial results.
     */
    public void testAFailedFetchOfAClusterNotSkippedFailsTheQuery() {
        String query = "FROM " + INDEX + "," + REMOTE_CLUSTER_1 + ":" + INDEX + " | SORT ts DESC | LIMIT 12 | KEEP ts, a, b";
        refuseFetches(REMOTE_CLUSTER_1);

        Exception e = expectThrows(Exception.class, () -> run(query, true, false).close());

        assertThat(e.getMessage(), containsString("fetch refused"));
    }

    /**
     * With partial results, a failed fetch of any cluster drops the rows of its shards.
     */
    public void testAFailedFetchWithPartialResultsDropsTheRowsOfItsCluster() {
        String query = "FROM " + INDEX + "," + REMOTE_CLUSTER_1 + ":" + INDEX + " | SORT ts DESC | LIMIT 12 | KEEP ts, a, b";
        List<List<Object>> eager = rows(query, false);
        refuseFetches(REMOTE_CLUSTER_1);

        try (EsqlQueryResponse response = run(query, true, true)) {
            List<List<Object>> fetched = EsqlTestUtils.getValuesList(response);
            assertThat(fetched, everyItem(rowOf(LOCAL_CLUSTER)));
            assertThat(fetched, hasSize(rowsOf(eager, LOCAL_CLUSTER)));
            EsqlExecutionInfo.Cluster remote = response.getExecutionInfo().getCluster(REMOTE_CLUSTER_1);
            assertThat(remote.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertThat(remote.getFailedShards(), greaterThan(0));
        }
    }

    /**
     * The branches of a {@code FORK} read the local cluster and a remote one, and share one execution of the lease, which
     * holds the contexts of every branch on every cluster. One fetch after the final cut loads the rows of both clusters.
     */
    public void testAForkFetchesTheRowsOfEveryBranchAndCluster() {
        String query = "FROM "
            + INDEX
            + ","
            + REMOTE_CLUSTER_1
            + ":"
            + INDEX
            + " | FORK (WHERE ts % 2 == 0 | SORT ts DESC | LIMIT 6) (WHERE ts % 5 == 0 | SORT ts DESC | LIMIT 6)"
            + " | SORT ts DESC, _fork | LIMIT 8 | KEEP ts, a, _fork";

        List<List<Object>> fetched = assertSameRowsAsTheEagerQuery(query, 8);

        assertThat(rowsOf(fetched, LOCAL_CLUSTER), greaterThan(0));
        assertThat(rowsOf(fetched, REMOTE_CLUSTER_1), greaterThan(0));
        assertThat(fetchRequests.get(REMOTE_CLUSTER_1).get(), greaterThan(0));
    }

    /**
     * {@code FUSE} groups the rows of the branches by {@code _id} and {@code _index}, and the {@code _index} of a remote
     * document names its cluster, so a document of each cluster is a group of its own.
     */
    public void testAFuseOverTheLocalAndARemoteClusterFetchesTheFinalWinners() {
        String query = "FROM "
            + INDEX
            + ","
            + REMOTE_CLUSTER_1
            + ":"
            + INDEX
            + " METADATA _id, _index, _score"
            + " | FORK (WHERE ts % 2 == 0 | SORT ts DESC | LIMIT 10) (WHERE ts % 3 == 0 | SORT ts DESC | LIMIT 10)"
            + " | FUSE | SORT _score DESC, ts DESC | LIMIT 6 | KEEP ts, a";

        List<List<Object>> fetched = assertSameRowsAsTheEagerQuery(query, 6);

        assertThat(rowsOf(fetched, LOCAL_CLUSTER), greaterThan(0));
        assertThat(rowsOf(fetched, REMOTE_CLUSTER_1), greaterThan(0));
    }

    /**
     * A remote data node that leaves after it received a fetch, before it answers, drops the rows of its shards when the
     * query skips its cluster on failure. The rows of the other shards and clusters come back in the order of the sort.
     */
    public void testARemoteNodeThatLeavesWhileItServesAFetchDropsTheRowsOfItsShards() throws Exception {
        String lost = startNodeWithWinners(REMOTE_CLUSTER_2);
        String query = "FROM "
            + INDEX
            + ","
            + REMOTE_CLUSTER_2
            + ":"
            + INDEX
            + ","
            + REMOTE_CLUSTER_2
            + ":"
            + LOST_INDEX
            + " | SORT ts DESC | LIMIT 12 | KEEP ts, a, b";
        List<List<Object>> eager = rows(query, false);
        CountDownLatch fetchArrived = neverAnswerFetches(REMOTE_CLUSTER_2, lost);

        ActionFuture<EsqlQueryResponse> fetching = client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request(query, true, false));
        safeAwait(fetchArrived);
        cluster(REMOTE_CLUSTER_2).stopNode(lost);

        try (EsqlQueryResponse response = fetching.actionGet(30, TimeUnit.SECONDS)) {
            List<List<Object>> kept = eager.stream().filter(row -> ((String) row.get(1)).startsWith(LOST_PREFIX) == false).toList();
            assertThat(EsqlTestUtils.getValuesList(response), equalTo(kept));
            assertTrue(response.isPartial());
            EsqlExecutionInfo.Cluster remote = response.getExecutionInfo().getCluster(REMOTE_CLUSTER_2);
            assertThat(remote.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertThat(remote.getFailures(), not(empty()));
            for (ShardSearchFailure failure : remote.getFailures()) {
                assertThat(failure.index(), equalTo(REMOTE_CLUSTER_2 + ":" + LOST_INDEX));
                assertThat(failure.getCause(), instanceOf(ConnectTransportException.class));
            }
        }
    }

    /**
     * A remote data node that leaves while it serves a fetch fails the query when the query doesn't skip its cluster on
     * failure and doesn't allow partial results.
     */
    public void testARemoteNodeThatLeavesWhileItServesAFetchFailsTheQueryOfAClusterNotSkipped() throws Exception {
        String lost = startNodeWithWinners(REMOTE_CLUSTER_1);
        String query = "FROM " + REMOTE_CLUSTER_1 + ":" + INDEX + "," + REMOTE_CLUSTER_1 + ":" + LOST_INDEX + " | SORT ts DESC | LIMIT 12";
        CountDownLatch fetchArrived = neverAnswerFetches(REMOTE_CLUSTER_1, lost);

        ActionFuture<EsqlQueryResponse> fetching = client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request(query, true, false));
        safeAwait(fetchArrived);
        cluster(REMOTE_CLUSTER_1).stopNode(lost);

        Exception e = expectThrows(Exception.class, () -> fetching.actionGet(30, TimeUnit.SECONDS).close());
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(ConnectTransportException.class));
    }

    /**
     * A remote cluster that the coordinator can no longer reach after the query phase drops its rows when the query skips
     * it on failure, like a fetch it refuses.
     */
    public void testARemoteClusterThatCanNoLongerBeReachedDropsItsRows() {
        String query = "FROM " + INDEX + "," + REMOTE_CLUSTER_2 + ":" + INDEX + " | SORT ts DESC | LIMIT 12 | KEEP ts, a, b";
        List<List<Object>> eager = rows(query, false);
        cannotReachTheFetchesOf(REMOTE_CLUSTER_2);

        try (EsqlQueryResponse response = run(query, true, false)) {
            List<List<Object>> localRows = eager.stream().filter(row -> ((String) row.get(1)).startsWith(prefix(LOCAL_CLUSTER))).toList();
            assertThat(EsqlTestUtils.getValuesList(response), equalTo(localRows));
            EsqlExecutionInfo.Cluster remote = response.getExecutionInfo().getCluster(REMOTE_CLUSTER_2);
            assertThat(remote.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertThat(remote.getFailures().getFirst().getCause(), instanceOf(ConnectTransportException.class));
        }
    }

    /**
     * Starts a data node in {@code clusterAlias} that holds {@link #LOST_INDEX} alone, whose documents sort above every
     * other, so they are among the winners of every cut. The other indices never move to it, so stopping it loses only
     * {@link #LOST_INDEX}.
     */
    private String startNodeWithWinners(String clusterAlias) {
        Client client = client(clusterAlias);
        assertAcked(
            client.admin()
                .indices()
                .prepareUpdateSettings(INDEX)
                .setSettings(Settings.builder().put("index.routing.allocation.exclude.fetch_lost", true))
        );
        String lost = cluster(clusterAlias).startNode(Settings.builder().put("node.attr.fetch_lost", true));
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(LOST_INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", lost)
                )
                .setMapping("ts", "type=long", "a", "type=keyword", "b", "type=long")
        );
        BulkRequestBuilder bulk = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 4; i++) {
            bulk.add(client.prepareIndex(LOST_INDEX).setSource(Map.of("ts", 10_000 + i, "a", LOST_PREFIX + i, "b", i)));
        }
        assertFalse(bulk.get().hasFailures());
        return lost;
    }

    /**
     * Swallows every fetch that {@code node} of {@code clusterAlias} receives, as if the node stopped answering, and counts
     * down once one arrives.
     */
    private CountDownLatch neverAnswerFetches(String clusterAlias, String node) {
        CountDownLatch fetchArrived = new CountDownLatch(1);
        ((MockTransportService) cluster(clusterAlias).getInstance(TransportService.class, node)).<
            TransportRequest>addRequestHandlingBehavior(
                FetchService.FETCH_ACTION_NAME,
                (handler, request, channel, task) -> fetchArrived.countDown()
            );
        return fetchArrived;
    }

    /**
     * Every node of the local cluster fails to send a fetch to a node of {@code clusterAlias}, directly or through a gateway,
     * as if the cluster went away after the query phase. The frees still go out, so no context waits for its keep-alive.
     */
    private void cannotReachTheFetchesOf(String clusterAlias) {
        Set<String> fetchActions = Set.of(
            FetchService.FETCH_ACTION_NAME,
            TransportActionProxy.getProxyAction(FetchService.FETCH_ACTION_NAME)
        );
        for (TransportService local : cluster(LOCAL_CLUSTER).getInstances(TransportService.class)) {
            for (TransportService remote : cluster(clusterAlias).getInstances(TransportService.class)) {
                ((MockTransportService) local).addSendBehavior(remote, (connection, requestId, action, request, options) -> {
                    if (fetchActions.contains(action)) {
                        throw new ConnectTransportException(connection.getNode(), "the remote cluster went away");
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
            }
        }
    }

    private void assertTheGatewayForwardsTheFetchesOfTheOtherNodes() throws Exception {
        spreadTheShardsOverTwoDataNodes(REMOTE_CLUSTER_1);
        String query = "FROM " + INDEX + "," + REMOTE_CLUSTER_1 + ":" + INDEX + " | SORT ts DESC | LIMIT 1000 | KEEP ts, a, b";
        int rows = DOCS_PER_CLUSTER * 2;

        assertSameRowsAsTheEagerQuery(query, rows);

        // two data nodes hold winners, and the coordinator is connected to one node only
        assertThat(forwardedFetchRequests.get(REMOTE_CLUSTER_1).get(), greaterThan(0));
    }

    /**
     * Starts a data node in {@code clusterAlias} when it has only one, and waits until the shards of {@link #INDEX} are on
     * two data nodes or more. The random clusters of the suite can have a single data node.
     */
    private void spreadTheShardsOverTwoDataNodes(String clusterAlias) throws Exception {
        InternalTestCluster cluster = cluster(clusterAlias);
        if (cluster.numDataNodes() < 2) {
            String node = cluster.startDataOnlyNode();
            countFetches(clusterAlias, (MockTransportService) cluster.getInstance(TransportService.class, node));
        }
        assertBusy(() -> {
            List<ShardRouting> shards = client(clusterAlias).admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .routingTable()
                .allShards(INDEX);
            assertThat(shards, everyItem(transformedMatch(ShardRouting::state, equalTo(ShardRoutingState.STARTED))));
            assertThat(shards.stream().map(ShardRouting::currentNodeId).distinct().count(), greaterThan(1L));
        });
    }

    /**
     * Runs {@code query} without and with the fetch phase, checks that both return the same rows, and that the coordinator
     * fetched {@code winners} documents.
     */
    private List<List<Object>> assertSameRowsAsTheEagerQuery(String query, int winners) {
        List<List<Object>> eager = rows(query, false);
        try (EsqlQueryResponse response = run(query, true, false)) {
            List<List<Object>> fetched = EsqlTestUtils.getValuesList(response);
            assertThat(fetched, equalTo(eager));
            assertThat("documents the coordinator fetched", fetchedDocuments(response.profile().drivers()), equalTo((long) winners));
            for (String clusterAlias : response.getExecutionInfo().clusterAliases()) {
                assertThat(
                    response.getExecutionInfo().getCluster(clusterAlias).getStatus(),
                    equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL)
                );
            }
            return fetched;
        }
    }

    private List<List<Object>> rows(String query, boolean fetchPhase) {
        try (EsqlQueryResponse response = run(query, fetchPhase, false)) {
            if (fetchPhase == false) {
                assertThat("the eager query fetches nothing", fetchedDocuments(response.profile().drivers()), equalTo(0L));
            }
            return EsqlTestUtils.getValuesList(response);
        }
    }

    private EsqlQueryResponse run(String query, boolean fetchPhase, boolean allowPartialResults) {
        return runQuery(request(query, fetchPhase, allowPartialResults));
    }

    private static EsqlQueryRequest request(String query, boolean fetchPhase, boolean allowPartialResults) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(query)
            .acceptedPragmaRisks(true)
            .pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_PHASE.getKey(), fetchPhase).build()))
            .profile(true)
            .allowPartialResults(allowPartialResults);
        request.includeCCSMetadata(true);
        return request;
    }

    /**
     * The documents the fetch operator of the coordinator asked for.
     */
    private static long fetchedDocuments(List<DriverProfile> drivers) {
        return drivers.stream()
            .flatMap(driver -> driver.operators().stream())
            .map(OperatorStatus::status)
            .filter(FetchOperator.Status.class::isInstance)
            .mapToLong(status -> ((FetchOperator.Status) status).documents())
            .sum();
    }

    /**
     * The data nodes of {@code clusterAlias} refuse every fetch, whichever node sends it on.
     */
    private void refuseFetches(String clusterAlias) {
        for (TransportService transportService : cluster(clusterAlias).getInstances(TransportService.class)) {
            ((MockTransportService) transportService).<TransportRequest>addRequestHandlingBehavior(
                FetchService.FETCH_ACTION_NAME,
                (handler, request, channel, task) -> channel.sendResponse(new ElasticsearchException("fetch refused"))
            );
        }
    }

    /**
     * Connects to the remote cluster {@code clusterAlias} in the mode {@code settings} name, and waits until it is connected.
     */
    private void updateRemoteCluster(String clusterAlias, String mode, Settings.Builder settings) throws Exception {
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(settings.put("cluster.remote." + clusterAlias + ".mode", mode))
        );
        assertBusy(() -> {
            List<RemoteConnectionInfo> connected = client(LOCAL_CLUSTER).execute(TransportRemoteInfoAction.TYPE, new RemoteInfoRequest())
                .actionGet()
                .getInfos()
                .stream()
                .filter(info -> info.getClusterAlias().equals(clusterAlias) && info.isConnected())
                .filter(info -> info.getModeInfo().modeName().equals(mode))
                .toList();
            assertThat(connected, hasSize(1));
        });
    }

    private static String prefix(String clusterAlias) {
        return (clusterAlias.isEmpty() ? "local" : clusterAlias) + "-";
    }

    /**
     * The rows of {@code clusterAlias} among {@code rows}, whose second column is {@code a}.
     */
    private static int rowsOf(List<List<Object>> rows, String clusterAlias) {
        return (int) rows.stream().filter(row -> ((String) row.get(1)).startsWith(prefix(clusterAlias))).count();
    }

    /**
     * A row of {@code clusterAlias}, whose second column is {@code a}.
     */
    private static Matcher<List<Object>> rowOf(String clusterAlias) {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(List<Object> row) {
                return ((String) row.get(1)).startsWith(prefix(clusterAlias));
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a row of [" + clusterAlias + "]");
            }
        };
    }

    private static List<List<Object>> sortedByA(List<List<Object>> rows) {
        List<List<Object>> sorted = new ArrayList<>(rows);
        sorted.sort(Comparator.comparing(row -> (String) row.get(0)));
        return sorted;
    }
}
