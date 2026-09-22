/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * An external scan must reassign splits when a worker is already unreachable
 * at dispatch, whether its connection fails or it has left the cluster state
 * since planning, and must not return a complete answer that omitted that
 * worker's files.
 */
public class ExternalDistributedNodeUnavailableIT extends AbstractExternalDataSourceIT {

    /**
     * Must stay at or above the eligible-worker count of any cluster this suite can build (shared data nodes
     * plus the nodes each test starts), because round-robin assignment hands a worker no splits once the
     * workers outnumber the files, and a worker with no splits is dropped before dispatch and never scans.
     * {@link #scanOn} asserts the invariant rather than leaving it to a topology-dependent flake.
     */
    private static final int FILES = 16;
    private static final int ROWS_PER_FILE = 100;
    private static final long ROWS = (long) FILES * ROWS_PER_FILE;
    /** Sum of ids 0 .. ROWS - 1; forces a real scan rather than an answer folded from file metadata. */
    private static final long ID_SUM = ROWS * (ROWS - 1) / 2;
    /** {@code DataNodeComputeHandler} is package-private in {@code ...esql.plugin}, so its logger is named by string. */
    private static final String DISPATCH_LOGGER = "org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler";

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testUnreachableNodeAtDispatchIsNotASilentPartialAnswer() throws Exception {
        runUnreachableNodeAtDispatch(true);
    }

    public void testUnreachableNodeAtDispatchIsReassignedWhenPartialResultsDisallowed() throws Exception {
        runUnreachableNodeAtDispatch(false);
    }

    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG",
        reason = "asserts the splits of the stopped node were reassigned rather than merely absent"
    )
    public void testNodeGoneFromClusterStateBetweenPlanningAndDispatchIsReassigned() throws Exception {
        // The coordinator and both workers are started here rather than picked from the shared cluster,
        // for two reasons. A node this test started is never one of InternalTestCluster's shared nodes,
        // so stopping a worker cannot trip the "only master eligible shared node" guard. And a data-only
        // node is never master-eligible, so the coordinator is never the elected master: were it the
        // master, its FollowersChecker would call getConnection on every worker once a second, on the
        // node's single scheduler thread, and that is the very call this test stalls — the check would
        // race the dispatch lookup for the one-shot stall below and park the scheduler in its place.
        List<String> started = internalCluster().startDataOnlyNodes(3);
        String coordinator = started.get(0);
        List<String> workers = started.subList(1, started.size());
        // Resolution walks the assignment map in eligible-node order, so stalling getConnection on
        // whichever worker comes first pauses it before the other worker is looked up — the window
        // this test needs. The coordinator's own getConnection is local and cannot be stalled.
        DiscoveryNode stall = null;
        DiscoveryNode unreachable = null;
        for (DiscoveryNode node : eligibleWorkers(coordinator)) {
            if (workers.contains(node.getName()) == false) {
                continue;
            }
            if (stall == null) {
                stall = node;
            } else {
                unreachable = node;
            }
        }
        assertThat(stall, notNullValue());
        assertThat(unreachable, notNullValue());

        DistributedScan scan = scanOn("unreachable_node_left_cluster", coordinator, unreachable.getName());
        try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(scan.query, true)).actionGet(TIMEOUT)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
            assertThat("the node stopped below must be assigned splits", externalScanNodeNames(response), hasItem(unreachable.getName()));
        }

        String unreachableId = unreachable.getId();
        // A one-shot handshake rather than a CyclicBarrier. Any coordinator-to-stall-node
        // getConnection runs this behavior, not only the dispatch lookup this test means to pause,
        // so a two-party barrier can rendezvous with the wrong caller and leave the dispatch thread
        // parked on one nobody else reaches. Only the first caller waits; the rest pass through, and
        // keeping the coordinator off the master role (above) is what makes that first caller the
        // dispatch lookup rather than a fault-detection check.
        CountDownLatch dispatchReachedStall = new CountDownLatch(1);
        CountDownLatch releaseDispatch = new CountDownLatch(1);
        AtomicBoolean stalled = new AtomicBoolean();
        var coordinatorTransport = MockTransportService.getInstance(coordinator);
        var stallAddress = internalCluster().getInstance(TransportService.class, stall.getName()).boundAddress().publishAddress();
        coordinatorTransport.addGetConnectionBehavior(stallAddress, (connectionManager, node) -> {
            if (stalled.compareAndSet(false, true)) {
                dispatchReachedStall.countDown();
                try {
                    // Deliberately not safeAwait: it signals failure with an AssertionError, which is
                    // not an Exception, so the dispatcher's catch would miss it and kill the query
                    // instead of reassigning the splits this test is about.
                    if (releaseDispatch.await(TIMEOUT.millis(), TimeUnit.MILLISECONDS) == false) {
                        throw new IllegalStateException("dispatch stall was never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return connectionManager.getConnection(node);
        });
        var faulted = request(scan.query, false);
        faulted.allowPartialResults(true);
        // The response numbers alone do not prove anything: they come out the same if the worker left
        // the cluster state before planning, in which case it was never assigned splits and the
        // reassignment under test never ran. Assert the dispatcher's own account of the reassignment,
        // so a missed window fails loudly instead of passing vacuously.
        try (var mockLog = MockLog.capture(DISPATCH_LOGGER)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "splits reassigned off the stopped worker",
                    DISPATCH_LOGGER,
                    Level.DEBUG,
                    "reassigned external splits from [1] unreachable nodes onto [*"
                )
            );
            ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(EsqlQueryAction.INSTANCE, faulted);
            boolean stopped = false;
            try {
                // TIMEOUT rather than the 10s default that ESTestCase#safeAwait(CountDownLatch) asks for:
                // the wait is for a query to reach dispatch on a possibly loaded CI worker, and failing
                // here abandons it mid-flight, leaking its exchange sinks and breaker usage into the next
                // test on this SUITE-shared cluster.
                safeAwait(dispatchReachedStall, TIMEOUT);
                assertThat(internalCluster().clusterService(coordinator).state().nodes().get(unreachableId), notNullValue());
                assertTrue(internalCluster().stopNode(unreachable.getName()));
                awaitClusterState(coordinator, state -> state.nodes().get(unreachableId) == null);
                stopped = true;
            } finally {
                // Unconditional: a dispatch thread parked on the stall must never outlive the test,
                // or the whole suite hangs until its timeout rather than reporting this test's failure.
                releaseDispatch.countDown();
                coordinatorTransport.clearAllRules();
                if (stopped == false) {
                    drainQuery(future);
                }
            }
            try (var response = future.actionGet(TIMEOUT)) {
                assertThat(response.isPartial(), equalTo(false));
                assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
            }
            mockLog.assertAllExpectationsMatched();
        }
    }

    private void runUnreachableNodeAtDispatch(boolean allowPartial) throws Exception {
        DistributedScan scan = distributedScan("unreachable_node_" + allowPartial);
        try (var response = client(scan.coordinator).execute(EsqlQueryAction.INSTANCE, request(scan.query, true)).actionGet(TIMEOUT)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
            assertThat(
                "the node made unreachable below must be assigned splits",
                externalScanNodeNames(response),
                hasItem(scan.unreachable)
            );
        }

        var coordinatorTransport = MockTransportService.getInstance(scan.coordinator);
        var unreachableAddress = internalCluster().getInstance(TransportService.class, scan.unreachable).boundAddress().publishAddress();
        coordinatorTransport.addGetConnectionBehavior(unreachableAddress, (connectionManager, node) -> {
            throw new NodeNotConnectedException(node, "simulated: node unreachable at dispatch");
        });
        var faulted = request(scan.query, false);
        faulted.allowPartialResults(allowPartial);
        try (var response = client(scan.coordinator).execute(EsqlQueryAction.INSTANCE, faulted).actionGet(TIMEOUT)) {
            assertThat(response.isPartial(), equalTo(false));
            assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
        } finally {
            coordinatorTransport.clearAllRules();
        }
    }

    private DistributedScan distributedScan(String datasetName) throws Exception {
        // Both nodes are started here, and a data-only node is never master-eligible, so the coordinator
        // is never the elected master. That matters because the caller's rule refuses every
        // coordinator-to-worker connection, not only the dispatch lookup: on a master coordinator it
        // would also fail the master's follower checks and get the worker voted out mid-query.
        List<String> started = internalCluster().startDataOnlyNodes(2);
        return scanOn(datasetName, started.get(0), started.get(1));
    }

    private DistributedScan scanOn(String datasetName, String coordinator, String unreachable) throws Exception {
        assertThat(
            "FILES must cover every eligible worker, or round-robin leaves one without splits and it never scans",
            eligibleWorkers(coordinator).size(),
            lessThanOrEqualTo(FILES)
        );
        Path root = createTempDir().resolve(datasetName);
        Files.createDirectories(root);
        for (int f = 0; f < FILES; f++) {
            StringBuilder body = new StringBuilder("id\n");
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                body.append(f * ROWS_PER_FILE + i).append('\n');
            }
            Files.writeString(root.resolve("part_" + f + ".csv"), body.toString(), StandardCharsets.UTF_8);
        }
        String dataset = registerDataset(datasetName, StoragePath.fileUri(root) + "/*.csv", Map.of());
        return new DistributedScan(coordinator, unreachable, "FROM " + dataset + " | STATS c = COUNT(*), s = SUM(id)");
    }

    private void drainQuery(ActionFuture<EsqlQueryResponse> future) {
        try {
            future.actionGet(TIMEOUT);
        } catch (Exception e) {
            logger.info("query ended while releasing the dispatch stall", e);
        }
    }

    private record DistributedScan(String coordinator, String unreachable, String query) {}

    private static EsqlQueryRequest request(String query, boolean profile) {
        var request = syncEsqlQueryRequest(query);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()));
        request.acceptedPragmaRisks(true);
        request.profile(profile);
        return request;
    }

    /** Same order and predicate as {@code NodeEligibilityStrategy.EXTERNAL_WORKER_NODES}. */
    private static List<DiscoveryNode> eligibleWorkers(String viaNode) {
        List<DiscoveryNode> eligible = new ArrayList<>();
        for (DiscoveryNode node : internalCluster().clusterService(viaNode).state().nodes()) {
            if (node.canContainData() && node.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()) == false) {
                eligible.add(node);
            }
        }
        return eligible;
    }
}
