/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
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
import java.util.Arrays;
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
import static org.hamcrest.Matchers.notNullValue;

/**
 * An external scan must reassign splits when a worker is already unreachable
 * at dispatch, whether its connection fails or it has left the cluster state
 * since planning, and must not return a complete answer that omitted that
 * worker's files.
 */
public class ExternalDistributedNodeUnavailableIT extends AbstractExternalDataSourceIT {

    private static final int FILES = 8;
    private static final int ROWS_PER_FILE = 100;
    private static final long ROWS = (long) FILES * ROWS_PER_FILE;
    /** Sum of ids 0 .. ROWS - 1; forces a real scan rather than an answer folded from file metadata. */
    private static final long ID_SUM = ROWS * (ROWS - 1) / 2;

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

    public void testNodeGoneFromClusterStateBetweenPlanningAndDispatchIsReassigned() throws Exception {
        // Both workers are started here rather than picked from the shared cluster. A node this test
        // started is never one of InternalTestCluster's shared nodes, so stopping it cannot trip the
        // "only master eligible shared node" guard, and a data-only node is never the master, so
        // holding up its connection cannot hold up cluster-state publication.
        List<String> workers = internalCluster().startDataOnlyNodes(2);
        String coordinator = dataNodeOutside(workers);
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
        // parked on one nobody else reaches. Only the first caller waits; the rest pass through.
        CountDownLatch dispatchReachedStall = new CountDownLatch(1);
        CountDownLatch releaseDispatch = new CountDownLatch(1);
        AtomicBoolean stallArmed = new AtomicBoolean(true);
        var coordinatorTransport = MockTransportService.getInstance(coordinator);
        var stallAddress = internalCluster().getInstance(TransportService.class, stall.getName()).boundAddress().publishAddress();
        coordinatorTransport.addGetConnectionBehavior(stallAddress, (connectionManager, node) -> {
            if (stallArmed.compareAndSet(true, false)) {
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
        ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(EsqlQueryAction.INSTANCE, faulted);
        boolean stopped = false;
        try {
            // TIMEOUT rather than the 10s safeAwait default: on a loaded CI worker the query can
            // need longer than that just to reach dispatch, and failing here abandons it mid-flight,
            // leaking its exchange sinks and breaker usage into the next test on this shared cluster.
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
        // A node started here is never the master, so refusing the coordinator's connection to it
        // cannot stall cluster-state publication and destabilize the rest of the suite.
        String unreachable = internalCluster().startDataOnlyNode();
        return scanOn(datasetName, dataNodeOutside(List.of(unreachable)), unreachable);
    }

    /** A data node this test did not start, to act as coordinator. */
    private static String dataNodeOutside(Collection<String> exclude) {
        return Arrays.stream(internalCluster().getNodeNames())
            .filter(name -> isDataNode(name) && exclude.contains(name) == false)
            .findFirst()
            .orElseThrow(() -> new AssertionError("no data node outside " + exclude));
    }

    private DistributedScan scanOn(String datasetName, String coordinator, String unreachable) throws Exception {
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
            logger.info("query ended while releasing the dispatch gap", e);
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

    private static boolean isDataNode(String nodeName) {
        return internalCluster().clusterService(nodeName).localNode().canContainData();
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
