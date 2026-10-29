/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.esql.datasources.DatasetConfigResolver;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler.ExternalDispatchResolution;
import org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler.ExternalDispatchResolution.ResolvedExternalNode;
import org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler.ExternalDispatchResolution.UnresolvedExternalNode;
import org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler.ExternalNodeConnectionLookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

/**
 * Dispatch-time resolution of external split assignments: an unreachable worker's
 * splits must land on a resolved worker instead of being dropped.
 */
public class DataNodeComputeHandlerExternalDispatchTests extends ESTestCase {

    public void testMissingNodeSplitsMoveOntoTheSurvivor() {
        List<ExternalSplit> all = splits(4);
        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        assignments.put("node-0", List.of(all.get(0), all.get(1)));
        assignments.put("node-1", List.of(all.get(2), all.get(3)));

        DiscoveryNode node0 = node("node-0");
        Transport.Connection connection0 = connection();
        Function<String, DiscoveryNode> nodes = id -> id.equals("node-0") ? node0 : null;
        ExternalNodeConnectionLookup connections = node -> {
            assertThat(node.getId(), equalTo("node-0"));
            return connection0;
        };

        ExternalDispatchResolution dispatched = dispatch(assignments, nodes, connections);

        assertThat(dispatched.unresolved(), empty());
        assertThat(dispatched.resolved(), hasSize(1));
        assertThat(dispatched.resolved().get(0).node(), equalTo(node0));
        assertThat(dispatched.resolved().get(0).connection(), equalTo(connection0));
        assertThat(dispatched.resolved().get(0).splits(), contains(all.toArray(ExternalSplit[]::new)));
    }

    public void testConnectionFailureSplitsMoveOntoTheSurvivor() {
        List<ExternalSplit> all = splits(4);
        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        assignments.put("node-0", List.of(all.get(0), all.get(1)));
        assignments.put("node-1", List.of(all.get(2), all.get(3)));

        DiscoveryNode node0 = node("node-0");
        DiscoveryNode node1 = node("node-1");
        Transport.Connection connection0 = connection();
        Map<String, DiscoveryNode> live = Map.of("node-0", node0, "node-1", node1);
        ExternalNodeConnectionLookup connections = node -> {
            if (node.getId().equals("node-1")) {
                throw new NodeNotConnectedException(node, "simulated: node unreachable at dispatch");
            }
            return connection0;
        };

        ExternalDispatchResolution dispatched = dispatch(assignments, live::get, connections);

        assertThat(dispatched.unresolved(), empty());
        assertThat(dispatched.resolved(), hasSize(1));
        assertThat(dispatched.resolved().get(0).node(), equalTo(node0));
        assertThat(dispatched.resolved().get(0).splits(), contains(all.toArray(ExternalSplit[]::new)));
    }

    public void testOrphansRoundRobinAcrossTwoSurvivors() {
        List<ExternalSplit> all = splits(4);
        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        assignments.put("node-0", List.of(all.get(0)));
        assignments.put("node-1", List.of(all.get(1), all.get(2)));
        assignments.put("node-2", List.of(all.get(3)));

        DiscoveryNode node0 = node("node-0");
        DiscoveryNode node2 = node("node-2");
        Transport.Connection connection0 = connection();
        Transport.Connection connection2 = connection();
        Function<String, DiscoveryNode> nodes = id -> switch (id) {
            case "node-0" -> node0;
            case "node-1" -> null;
            case "node-2" -> node2;
            default -> throw new AssertionError("unexpected node id: " + id);
        };
        ExternalNodeConnectionLookup connections = node -> node.getId().equals("node-0") ? connection0 : connection2;

        ExternalDispatchResolution dispatched = dispatch(assignments, nodes, connections);

        assertThat(dispatched.unresolved(), empty());
        assertThat(dispatched.resolved(), hasSize(2));
        Map<String, List<ExternalSplit>> byNode = byNodeId(dispatched);
        assertThat(byNode.get("node-0"), contains(all.get(0), all.get(1)));
        assertThat(byNode.get("node-2"), contains(all.get(3), all.get(2)));
        assertThat(allSplits(dispatched), containsInAnyOrder(all.toArray(ExternalSplit[]::new)));
    }

    public void testAllUnreachableLeavesAssignmentsUnresolved() {
        List<ExternalSplit> all = splits(2);
        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        assignments.put("node-0", List.of(all.get(0)));
        assignments.put("node-1", List.of(all.get(1)));

        ExternalDispatchResolution dispatched = dispatch(assignments, id -> null, node -> {
            throw new AssertionError("no node should be connected");
        });

        assertThat(dispatched.resolved(), empty());
        assertThat(dispatched.unresolved(), hasSize(2));
        assertThat(
            dispatched.unresolved().stream().flatMap(u -> u.splits().stream()).toList(),
            contains(all.toArray(ExternalSplit[]::new))
        );
    }

    public void testAllReachableLeavesAssignmentsUnchanged() {
        List<ExternalSplit> all = splits(3);
        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        assignments.put("node-0", List.of(all.get(0)));
        assignments.put("node-1", List.of(all.get(1), all.get(2)));

        DiscoveryNode node0 = node("node-0");
        DiscoveryNode node1 = node("node-1");
        Transport.Connection connection0 = connection();
        Transport.Connection connection1 = connection();
        Function<String, DiscoveryNode> nodes = id -> id.equals("node-0") ? node0 : node1;
        ExternalNodeConnectionLookup connections = node -> node.getId().equals("node-0") ? connection0 : connection1;

        ExternalDispatchResolution dispatched = dispatch(assignments, nodes, connections);

        assertThat(dispatched.unresolved(), empty());
        assertThat(dispatched.resolved(), hasSize(2));
        Map<String, List<ExternalSplit>> byNode = byNodeId(dispatched);
        assertThat(byNode.get("node-0"), contains(all.get(0)));
        assertThat(byNode.get("node-1"), contains(all.get(1), all.get(2)));
    }

    public void testEmptyLiveAssignmentAbsorbsOrphansFromUnreachableNode() {
        List<ExternalSplit> all = splits(2);
        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        assignments.put("node-0", List.of());
        assignments.put("node-1", List.of(all.get(0), all.get(1)));

        DiscoveryNode node0 = node("node-0");
        Function<String, DiscoveryNode> nodes = id -> switch (id) {
            case "node-0" -> node0;
            case "node-1" -> node(id);
            default -> throw new AssertionError("unexpected node id: " + id);
        };
        ExternalNodeConnectionLookup connections = node -> {
            if (node.getId().equals("node-1")) {
                throw new NodeNotConnectedException(node, "simulated: node unreachable at dispatch");
            }
            return connection();
        };

        ExternalDispatchResolution dispatched = dispatch(assignments, nodes, connections);

        assertThat(dispatched.unresolved(), empty());
        assertThat(dispatched.resolved(), hasSize(1));
        assertThat(dispatched.resolved().get(0).node(), equalTo(node0));
        assertThat(dispatched.resolved().get(0).splits(), contains(all.toArray(ExternalSplit[]::new)));
    }

    public void testAllUnreachableFailureKeepsPerNodeErrors() {
        NodeNotConnectedException first = new NodeNotConnectedException(node("node-0"), "first");
        NodeNotConnectedException second = new NodeNotConnectedException(node("node-1"), "second");
        IllegalStateException failure = DataNodeComputeHandler.allExternalWorkersFailed(
            List.of(new UnresolvedExternalNode("node-0", splits(1), first), new UnresolvedExternalNode("node-1", splits(1), second))
        );

        assertThat(failure.getMessage(), equalTo("all [2] nodes assigned external splits failed"));
        assertThat(failure.getCause(), sameInstance(first));
        assertThat(failure.getSuppressed(), arrayContaining(instanceOf(NodeNotConnectedException.class)));
        assertThat(failure.getSuppressed()[0], sameInstance(second));
    }

    public void testEmptyAssignmentWithNoOrphansIsDropped() {
        List<ExternalSplit> all = splits(1);
        Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
        assignments.put("node-empty", List.of());
        assignments.put("node-0", List.of(all.get(0)));

        DiscoveryNode node0 = node("node-0");
        Transport.Connection connection0 = connection();

        ExternalDispatchResolution dispatched = dispatch(assignments, id -> id.equals("node-0") ? node0 : node(id), node -> connection0);

        assertThat(dispatched.unresolved(), empty());
        assertThat(dispatched.resolved(), hasSize(1));
        assertThat(dispatched.resolved().get(0).splits(), contains(all.get(0)));
    }

    /**
     * A worker that declines its splits hands them to the next resolved worker, wrapping around, so that a walk visits every
     * other worker once and concurrent leaves whose placement the strategy offset do not all fall back onto the same node;
     * a lone worker has nowhere to go and waits.
     */
    public void testWalkVisitsEveryOtherWorkerOnceInRingOrder() {
        ResolvedExternalNode node0 = new ResolvedExternalNode(node("node-0"), connection(), splits(1));
        ResolvedExternalNode node1 = new ResolvedExternalNode(node("node-1"), connection(), splits(1));
        ResolvedExternalNode node2 = new ResolvedExternalNode(node("node-2"), connection(), splits(1));
        List<ResolvedExternalNode> resolved = List.of(node0, node1, node2);

        assertThat(DataNodeComputeHandler.nextWorker(resolved, 0, 1), sameInstance(node1));
        assertThat(DataNodeComputeHandler.nextWorker(resolved, 0, 2), sameInstance(node2));
        assertNull(DataNodeComputeHandler.nextWorker(resolved, 0, 3));
        assertThat(DataNodeComputeHandler.nextWorker(resolved, 1, 1), sameInstance(node2));
        assertThat(DataNodeComputeHandler.nextWorker(resolved, 1, 2), sameInstance(node0));
        assertThat(DataNodeComputeHandler.nextWorker(resolved, 2, 1), sameInstance(node0));
        assertThat(DataNodeComputeHandler.nextWorker(resolved, 2, 2), sameInstance(node1));
        assertNull(DataNodeComputeHandler.nextWorker(resolved, 2, 0));
        assertThat(DataNodeComputeHandler.nextWorker(List.of(node0, node1), 1, 1), sameInstance(node0));
        assertNull(DataNodeComputeHandler.nextWorker(List.of(node0, node1), 1, 2));
        assertNull(DataNodeComputeHandler.nextWorker(List.of(node0), 0, 1));
    }

    /** Every attempt of a walk but its last asks the worker to decline when not ready; the last, or a lone worker, waits. */
    public void testOnlyTheLastAttemptOfAWalkWaits() {
        assertFalse(DataNodeComputeHandler.declineIfNotReady(0, 1));
        assertTrue(DataNodeComputeHandler.declineIfNotReady(0, 2));
        assertFalse(DataNodeComputeHandler.declineIfNotReady(1, 2));
        assertTrue(DataNodeComputeHandler.declineIfNotReady(0, 3));
        assertTrue(DataNodeComputeHandler.declineIfNotReady(1, 3));
        assertFalse(DataNodeComputeHandler.declineIfNotReady(2, 3));
    }

    /**
     * A dataset refusal is lag only when the worker's applied state is older than the one the coordinator planned from;
     * a refusal on a state at or above it is a real miss, an old coordinator (no version) is never second-guessed, and
     * nothing but a 400 status exception counts as a refusal.
     */
    public void testDatasetRefusalWhileBehind() {
        ElasticsearchStatusException refusal = DatasetConfigResolver.notRegistered("hits");
        long required = randomLongBetween(2, 1000);

        assertTrue(DataNodeComputeHandler.isDatasetRefusalWhileBehind(refusal, request(required), state(required - 1)));
        assertTrue(DataNodeComputeHandler.isDatasetRefusalWhileBehind(refusal, request(required), state(0)));
        assertFalse(DataNodeComputeHandler.isDatasetRefusalWhileBehind(refusal, request(required), state(required)));
        assertFalse(DataNodeComputeHandler.isDatasetRefusalWhileBehind(refusal, request(required), state(required + 1)));
        assertFalse(
            "an old coordinator sends no version",
            DataNodeComputeHandler.isDatasetRefusalWhileBehind(refusal, request(0), state(0))
        );

        Exception[] notRefusals = new Exception[] {
            new ElasticsearchStatusException("forbidden", RestStatus.FORBIDDEN),
            new ElasticsearchStatusException("server", RestStatus.INTERNAL_SERVER_ERROR),
            new ElasticsearchTimeoutException("timed out while waiting for cluster to unblock"),
            new ClusterBlockException(Set.of(GatewayService.STATE_NOT_RECOVERED_BLOCK)),
            new TaskCancelledException("cancelled"),
            new IllegalArgumentException("bad glob") };
        for (Exception e : notRefusals) {
            assertFalse(e.toString(), DataNodeComputeHandler.isDatasetRefusalWhileBehind(e, request(required), state(required - 1)));
        }
    }

    private static DataNodeRequest request(long requiredClusterStateVersion) {
        // No plan: the readiness predicate reads nothing but the version, as DataNodeRequestTests does for the indices.
        return new DataNodeRequest(
            "session",
            TaskId.EMPTY_TASK_ID,
            randomConfiguration("from test", randomTables()),
            "",
            List.of(),
            Map.of(),
            null,
            new String[0],
            IndicesOptions.STRICT_EXPAND_OPEN,
            false,
            false,
            false,
            splits(1),
            randomBoolean(),
            requiredClusterStateVersion
        );
    }

    private static ClusterState state(long version) {
        return ClusterState.builder(ClusterName.DEFAULT).version(version).build();
    }

    private static ExternalDispatchResolution dispatch(
        Map<String, List<ExternalSplit>> assignments,
        Function<String, DiscoveryNode> nodes,
        ExternalNodeConnectionLookup connections
    ) {
        return DataNodeComputeHandler.reassignUnreachableSplits(
            DataNodeComputeHandler.resolveExternalAssignments(assignments, nodes, connections)
        );
    }

    private static Map<String, List<ExternalSplit>> byNodeId(ExternalDispatchResolution resolution) {
        Map<String, List<ExternalSplit>> byNode = new HashMap<>();
        for (ResolvedExternalNode resolved : resolution.resolved()) {
            byNode.put(resolved.node().getId(), resolved.splits());
        }
        return byNode;
    }

    private static List<ExternalSplit> allSplits(ExternalDispatchResolution resolution) {
        return resolution.resolved().stream().flatMap(r -> r.splits().stream()).collect(Collectors.toList());
    }

    private static List<ExternalSplit> splits(int count) {
        List<ExternalSplit> splits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            splits.add(
                new FileSplit("parquet", StoragePath.of("s3://bucket/file" + i + ".parquet"), 0, 1024, "parquet", Map.of(), Map.of())
            );
        }
        return splits;
    }

    private static DiscoveryNode node(String id) {
        return DiscoveryNodeUtils.builder(id).name(id).roles(Set.of(DATA_HOT_NODE_ROLE)).build();
    }

    /**
     * Identity token only: {@link Transport.Connection} is a large interface and these tests
     * never send on it.
     */
    private static Transport.Connection connection() {
        return mock(Transport.Connection.class);
    }
}
