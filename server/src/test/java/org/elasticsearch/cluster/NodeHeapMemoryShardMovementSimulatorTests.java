/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class NodeHeapMemoryShardMovementSimulatorTests extends ESAllocationTestCase {

    private static final long TOTAL_HEAP_BYTES = 2000L;

    public void testNoDeltasReturnsSameMetricsReference() {
        var nodeId = "node-0";
        var initialMetrics = Map.of(nodeId, nodeHeapMetrics(nodeId, randomIntBetween(100, 300), randomIntBetween(0, 100)));
        var simulator = newSimulator(initialMetrics, Map.of(), ShardAndIndexHeapUsage.ZERO, emptyRoutingNodes());

        assertThat(simulator.getSimulatedHeapMetrics(), sameInstance(initialMetrics));
    }

    public void testRelocationRecomputesHeapFromFinalPlacement() {
        var nodeA = "node-a";
        var nodeB = "node-b";
        var state = buildSingleShardState("test-index", nodeA, nodeB);
        var routingNodes = state.mutableRoutingNodes();
        var startedShard = getSoleStartedShard(routingNodes, nodeA);
        final long sourceBaseline = randomLongBetween(10, 100);
        final long targetBaseline = randomLongBetween(10, 100);
        final long shardHeap = randomLongBetween(51, 100);
        final long indexHeap = randomLongBetween(31, 50);
        final var shardAndIndexHeap = new ShardAndIndexHeapUsage(shardHeap, indexHeap);
        // Keep the non-shard baseline explicit: the simulator should move only the placement-derived shard and index heap.
        final var initialMetrics = Map.of(
            nodeA,
            nodeHeapMetrics(nodeA, sourceBaseline + shardHeap + indexHeap, shardHeap),
            nodeB,
            nodeHeapMetrics(nodeB, targetBaseline, 0)
        );
        final var simulator = newSimulator(
            initialMetrics,
            Map.of(startedShard.shardId(), shardAndIndexHeap),
            ShardAndIndexHeapUsage.ZERO,
            routingNodes
        );

        var result = simulator.getSimulatedHeapMetrics();
        assertThat(result, sameInstance(initialMetrics));
        assertThat(result.get(nodeA).nodeHeapEstimates().totalHeapUsage(), equalTo(sourceBaseline + shardHeap + indexHeap));
        assertThat(result.get(nodeA).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(shardHeap));
        assertThat(result.get(nodeB).nodeHeapEstimates().totalHeapUsage(), equalTo(targetBaseline));
        assertThat(result.get(nodeB).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(0L));

        var relocationShards = routingNodes.relocateShard(
            startedShard,
            nodeB,
            0,
            "test",
            RoutingChangesObserver.NOOP,
            ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
        );

        simulator.simulateShardStarted(relocationShards.v2());

        result = simulator.getSimulatedHeapMetrics();
        // After relocation, source keeps only its non-shard baseline; target gains both the shard heap and first index heap.
        assertThat(result.get(nodeA).nodeHeapEstimates().totalHeapUsage(), equalTo(sourceBaseline));
        assertThat(result.get(nodeA).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(0L));
        assertThat(result.get(nodeB).nodeHeapEstimates().totalHeapUsage(), equalTo(targetBaseline + shardHeap + indexHeap));
        assertThat(result.get(nodeB).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(shardHeap));
    }

    /** Nodes not present in the initial metrics map are silently skipped; results for known nodes are unaffected. */
    public void testNodeWithoutInitialMetricsIsSkipped() {
        var nodeA = "node-a";
        var nodeB = "node-b";
        var state = buildSingleShardState("test-index", nodeA, nodeB);
        var routingNodes = state.mutableRoutingNodes();
        var startedShard = getSoleStartedShard(routingNodes, nodeA);
        var relocationShards = routingNodes.relocateShard(
            startedShard,
            nodeB,
            0,
            "test",
            RoutingChangesObserver.NOOP,
            ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
        );

        // Neither nodeA nor nodeB has initial metrics
        var simulator = newSimulator(
            Map.of(),
            Map.of(startedShard.shardId(), new ShardAndIndexHeapUsage(100, 50)),
            ShardAndIndexHeapUsage.ZERO,
            routingNodes
        );

        simulator.simulateShardStarted(relocationShards.v2());
        assertThat(simulator.getSimulatedHeapMetrics().size(), equalTo(0));
    }

    public void testIndexHeapUsageIsCountedOncePerNode() {
        var nodeA = "node-a";
        var nodeB = "node-b";
        var state = buildTwoShardState("test-index", nodeA, nodeB);
        var routingNodes = state.mutableRoutingNodes();
        var shard0 = getStartedShardById(routingNodes, nodeA, 0);
        var shard1 = getStartedShardById(routingNodes, nodeA, 1);
        final long baseline = randomLongBetween(10, 100);
        final long shardHeap = randomLongBetween(51, 100);
        final long indexHeap = randomLongBetween(31, 50);
        final var shardAndIndexHeap = new ShardAndIndexHeapUsage(shardHeap, indexHeap);
        // nodeA starts with two shards from one index, so index heap is counted once, not once per shard.
        final var initialMetrics = Map.of(
            nodeA,
            nodeHeapMetrics(nodeA, baseline + 2 * shardHeap + indexHeap, 2 * shardHeap),
            nodeB,
            nodeHeapMetrics(nodeB, baseline, 0)
        );
        final var simulator = newSimulator(
            initialMetrics,
            Map.of(shard0.shardId(), shardAndIndexHeap, shard1.shardId(), shardAndIndexHeap),
            ShardAndIndexHeapUsage.ZERO,
            routingNodes
        );

        var result = simulator.getSimulatedHeapMetrics();
        assertThat(result, sameInstance(initialMetrics));
        assertThat(result.get(nodeA).nodeHeapEstimates().totalHeapUsage(), equalTo(baseline + 2 * shardHeap + indexHeap));
        assertThat(result.get(nodeA).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(2 * shardHeap));
        assertThat(result.get(nodeB).nodeHeapEstimates().totalHeapUsage(), equalTo(baseline));
        assertThat(result.get(nodeB).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(0L));

        var firstRelocation = routingNodes.relocateShard(
            shard0,
            nodeB,
            0,
            "test",
            RoutingChangesObserver.NOOP,
            ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
        );
        simulator.simulateShardStarted(firstRelocation.v2());
        routingNodes.startShard(firstRelocation.v2(), RoutingChangesObserver.NOOP, 0L);
        result = simulator.getSimulatedHeapMetrics();
        // Both nodes now host the index, so each node includes one copy of the index heap.
        assertThat(result.get(nodeA).nodeHeapEstimates().totalHeapUsage(), equalTo(baseline + shardHeap + indexHeap));
        assertThat(result.get(nodeA).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(shardHeap));
        assertThat(result.get(nodeB).nodeHeapEstimates().totalHeapUsage(), equalTo(baseline + shardHeap + indexHeap));
        assertThat(result.get(nodeB).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(shardHeap));

        var secondRelocation = routingNodes.relocateShard(
            shard1,
            nodeB,
            0,
            "test",
            RoutingChangesObserver.NOOP,
            ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
        );
        simulator.simulateShardStarted(secondRelocation.v2());
        result = simulator.getSimulatedHeapMetrics();
        // Moving the last shard away removes the source's index heap; the target still counts that index heap only once.
        assertThat(result.get(nodeA).nodeHeapEstimates().totalHeapUsage(), equalTo(baseline));
        assertThat(result.get(nodeA).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(0L));
        assertThat(result.get(nodeB).nodeHeapEstimates().totalHeapUsage(), equalTo(baseline + 2 * shardHeap + indexHeap));
        assertThat(result.get(nodeB).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(2 * shardHeap));
    }

    public void testMissingShardEstimateUsesDefaultHeapUsage() {
        var nodeId = "node-a";
        final long baseline = randomLongBetween(10, 100);
        final long shardHeap = randomLongBetween(51, 100);
        final long indexHeap = randomLongBetween(31, 50);
        final var simulator = newSimulator(
            Map.of(nodeId, nodeHeapMetrics(nodeId, baseline, 0)),
            Map.of(),
            new ShardAndIndexHeapUsage(shardHeap, indexHeap),
            emptyRoutingNodes()
        );

        simulator.simulateShardStarted(newInitializingShard(new ShardId("test-index", "_na_", 0), nodeId));

        var result = simulator.getSimulatedHeapMetrics();
        assertThat(result.get(nodeId).nodeHeapEstimates().totalHeapUsage(), equalTo(baseline + shardHeap + indexHeap));
        assertThat(result.get(nodeId).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(shardHeap));
    }

    public void testAlreadyReflectedMoveIsNotAppliedAgain() {
        var sourceNode = "node-a";
        var targetNode = "node-b";
        var state = buildSingleShardState("test-index", targetNode, sourceNode);
        var routingNodes = state.mutableRoutingNodes();
        var startedShard = getSoleStartedShard(routingNodes, targetNode);
        final long sourceBaseline = randomLongBetween(10, 100);
        final long targetBaseline = randomLongBetween(10, 100);
        final long shardHeap = randomLongBetween(51, 100);
        final long indexHeap = randomLongBetween(31, 50);
        final var initialMetrics = Map.of(
            sourceNode,
            nodeHeapMetrics(sourceNode, sourceBaseline, 0),
            targetNode,
            nodeHeapMetrics(targetNode, targetBaseline + shardHeap + indexHeap, shardHeap)
        );
        final var simulator = newSimulator(
            initialMetrics,
            Map.of(startedShard.shardId(), new ShardAndIndexHeapUsage(shardHeap, indexHeap)),
            ShardAndIndexHeapUsage.ZERO,
            routingNodes
        );

        simulator.simulateShardStarted(newRelocatingTargetShard(startedShard.shardId(), sourceNode, targetNode));
        assertThat(simulator.getSimulatedHeapMetrics(), sameInstance(initialMetrics));
    }

    // --- helpers ---

    private static NodeHeapMetrics nodeHeapMetrics(String nodeId, long totalHeap, long hostedShardsHeap) {
        return new NodeHeapMetrics(nodeId, TOTAL_HEAP_BYTES, new NodeHeapEstimates(totalHeap, hostedShardsHeap));
    }

    private static NodeHeapMemoryShardMovementSimulator newSimulator(
        Map<String, NodeHeapMetrics> initialMetrics,
        Map<ShardId, ShardAndIndexHeapUsage> shardHeapUsages,
        ShardAndIndexHeapUsage defaultHeapUsage,
        RoutingNodes routingNodes
    ) {
        return new NodeHeapMemoryShardMovementSimulator(initialMetrics, shardHeapUsages, defaultHeapUsage, routingNodes);
    }

    private static RoutingNodes emptyRoutingNodes() {
        return RoutingNodes.immutable(GlobalRoutingTable.EMPTY_ROUTING_TABLE, DiscoveryNodes.EMPTY_NODES);
    }

    private static ShardRouting newInitializingShard(ShardId shardId, String nodeId) {
        return shardRoutingBuilder(shardId, nodeId, true, INITIALIZING).withRecoverySource(RecoverySource.EmptyStoreRecoverySource.INSTANCE)
            .build();
    }

    private static ShardRouting newRelocatingTargetShard(ShardId shardId, String fromNodeId, String toNodeId) {
        return shardRoutingBuilder(shardId, toNodeId, true, INITIALIZING).withRelocatingNodeId(fromNodeId)
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .build();
    }

    /**
     * Builds a cluster state with a single-shard index, with the primary started on {@code primaryNode}
     * and {@code otherNode} present but holding no shards.
     */
    private ClusterState buildSingleShardState(String indexName, String primaryNode, String otherNode) {
        var indexMetadata = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        var primaryShard = newShardRouting(new ShardId(indexMetadata.getIndex(), 0), primaryNode, true, STARTED);
        return clusterStateWithShards(indexMetadata, primaryNode, otherNode, primaryShard);
    }

    /**
     * Builds a cluster state with a two-shard index, with both primaries started on {@code primaryNode}
     * and {@code otherNode} present but holding no shards.
     */
    private ClusterState buildTwoShardState(String indexName, String primaryNode, String otherNode) {
        var indexMetadata = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        var shard0 = newShardRouting(new ShardId(indexMetadata.getIndex(), 0), primaryNode, true, STARTED);
        var shard1 = newShardRouting(new ShardId(indexMetadata.getIndex(), 1), primaryNode, true, STARTED);
        return clusterStateWithShards(indexMetadata, primaryNode, otherNode, shard0, shard1);
    }

    private ClusterState clusterStateWithShards(IndexMetadata indexMetadata, String primaryNode, String otherNode, ShardRouting... shards) {
        var irtBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (var shard : shards) {
            irtBuilder.addShard(shard);
        }
        var routingTable = RoutingTable.builder().add(irtBuilder.build()).build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode(primaryNode)).add(newNode(otherNode)).build())
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(routingTable)
            .build();
    }

    private static ShardRouting getSoleStartedShard(RoutingNodes routingNodes, String nodeId) {
        var it = routingNodes.node(nodeId).iterator();
        assertTrue(it.hasNext());
        var shard = it.next();
        assertFalse(it.hasNext());
        return shard;
    }

    private static ShardRouting getStartedShardById(RoutingNodes routingNodes, String nodeId, int shardNum) {
        for (var shard : routingNodes.node(nodeId)) {
            if (shard.shardId().id() == shardNum && shard.started()) {
                return shard;
            }
        }
        throw new AssertionError("no started shard with id " + shardNum + " on node " + nodeId);
    }
}
