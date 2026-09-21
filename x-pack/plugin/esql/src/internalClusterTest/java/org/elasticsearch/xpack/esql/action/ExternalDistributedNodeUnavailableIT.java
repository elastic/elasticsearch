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
import java.util.concurrent.CyclicBarrier;

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
        internalCluster().ensureAtLeastNumDataNodes(3);
        String coordinator = internalCluster().getRandomNodeName();
        // Resolution walks this list in order. Stalling getConnection on the first remote worker
        // pauses after that worker's lookup and before every later remote worker. The coordinator's
        // own getConnection is local and cannot be stalled.
        List<DiscoveryNode> eligible = eligibleWorkers(coordinator);
        DiscoveryNode stall = null;
        DiscoveryNode unreachable = null;
        for (DiscoveryNode node : eligible) {
            if (node.getName().equals(coordinator)) {
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
        CyclicBarrier gap = new CyclicBarrier(2);
        var coordinatorTransport = MockTransportService.getInstance(coordinator);
        var stallAddress = internalCluster().getInstance(TransportService.class, stall.getName()).boundAddress().publishAddress();
        coordinatorTransport.addGetConnectionBehavior(stallAddress, (connectionManager, node) -> {
            try {
                safeAwait(gap);
                safeAwait(gap);
            } catch (AssertionError e) {
                // safeAwait fails the test thread with AssertionError. On this search thread that
                // error would skip the query listener, so surface it as a query failure instead.
                throw new IllegalStateException(e);
            }
            return connectionManager.getConnection(node);
        });
        var faulted = request(scan.query, false);
        faulted.allowPartialResults(true);
        ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(EsqlQueryAction.INSTANCE, faulted);
        boolean released = false;
        try {
            safeAwait(gap);
            assertThat(internalCluster().clusterService(coordinator).state().nodes().get(unreachableId), notNullValue());
            assertTrue(internalCluster().stopNode(unreachable.getName()));
            awaitClusterState(coordinator, state -> state.nodes().get(unreachableId) == null);
            safeAwait(gap);
            released = true;
            try (var response = future.actionGet(TIMEOUT)) {
                assertThat(response.isPartial(), equalTo(false));
                assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
            }
        } finally {
            coordinatorTransport.clearAllRules();
            if (released == false) {
                gap.reset();
                drainQuery(future);
            }
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
        internalCluster().ensureAtLeastNumDataNodes(2);
        List<String> dataNodes = Arrays.stream(internalCluster().getNodeNames()).filter(n -> isDataNode(n)).toList();
        return scanOn(datasetName, dataNodes.get(0), dataNodes.get(1));
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
