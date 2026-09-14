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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class NodeHeapMemoryShardMovementSimulatorTests extends ESAllocationTestCase {

    private static final long TOTAL_HEAP_BYTES = 2000L;

    /** No deltas → returns the same initial map reference (fast path). */
    public void testNoDeltasReturnsSameMetricsReference() {
        var nodeId = "node-0";
        var initialMetrics = Map.of(nodeId, nodeHeapMetrics(nodeId, randomIntBetween(100, 300), randomIntBetween(0, 100)));
        var simulator = newSimulator(initialMetrics, Map.of(), ShardAndIndexHeapUsage.ZERO, routingNodes(nodeId));

        assertThat(simulator.getSimulatedHeapMetrics(), sameInstance(initialMetrics));
    }

    /**
     * When removing the last shard of an index from a node, and the cumulative heap delta (shard + index) exceeds
     * the initial heap values, both totalHeapUsage and hostedShardsHeapUsage are clamped to 0.
     */
    public void testNegativeHeapUsageClampsToZeroForBothMetrics() {
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

        long shardHeap = randomLongBetween(51, 100), indexHeap = randomLongBetween(31, 50);
        // nodeA initial heap values are less than the shard+index heap that will be removed
        var initialMetrics = Map.of(nodeA, nodeHeapMetrics(nodeA, 50, 30), nodeB, nodeHeapMetrics(nodeB, 0, 0));
        var simulator = newSimulator(
            initialMetrics,
            Map.of(startedShard.shardId(), new ShardAndIndexHeapUsage(shardHeap, indexHeap)),
            ShardAndIndexHeapUsage.ZERO,
            routingNodes
        );

        simulator.simulateShardStarted(relocationShards.v2(), true);

        var result = simulator.getSimulatedHeapMetrics();
        // nodeA: remove shardHeap + indexHeap > 82 delta; initial total=50 → max(0, 50 - shardHeap - indexHeap) = 0
        assertThat(result.get(nodeA).nodeHeapEstimates().totalHeapUsage(), equalTo(0L));
        // nodeA: remove shardHeap + indexHeap; initial hosted=30 → max(0, 30 - shardHeap - indexHeap) = 0
        assertThat(result.get(nodeA).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(0L));
        // nodeB: add shardHeap + indexHeap; initial total=0 → shardHeap + indexHeap
        assertThat(result.get(nodeB).nodeHeapEstimates().totalHeapUsage(), equalTo(shardHeap + indexHeap));
        // nodeB: add shardHeap + indexHeap; initial hosted=0 → shardHeap + indexHeap
        assertThat(result.get(nodeB).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(shardHeap + indexHeap));
    }

    /**
     * hostedShardsHeapUsage can clamp to 0 independently of totalHeapUsage. When a non-last shard is removed
     * from a node (so index heap is not subtracted), only the shard heap delta applies to hostedShardsHeapUsage.
     * If hostedShardsHeapUsage is smaller than the shard heap delta, it clamps while totalHeapUsage does not.
     */
    public void testHostedShardsHeapClampsIndependentlyFromTotalHeap() {
        var nodeA = "node-a";
        var nodeB = "node-b";
        // Index has 2 primaries, both on nodeA. Relocating only shard 0 leaves shard 1 on nodeA,
        // so numberOfOwningShardsForIndex(nodeA) == 1 → no index heap delta, only shard heap delta.
        var state = buildTwoShardState("test-index", nodeA, nodeB);
        var routingNodes = state.mutableRoutingNodes();
        var shard0 = getStartedShardById(routingNodes, nodeA, 0);
        var relocationShards = routingNodes.relocateShard(
            shard0,
            nodeB,
            0,
            "test",
            RoutingChangesObserver.NOOP,
            ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
        );

        long shardHeap = 100L, indexHeap = 50L;
        var initialMetrics = Map.of(
            nodeA,
            nodeHeapMetrics(nodeA, 200, 30),  // total high (won't clamp), hosted low (will clamp)
            nodeB,
            nodeHeapMetrics(nodeB, 0, 0)
        );
        var simulator = newSimulator(
            initialMetrics,
            Map.of(shard0.shardId(), new ShardAndIndexHeapUsage(shardHeap, indexHeap)),
            ShardAndIndexHeapUsage.ZERO,
            routingNodes
        );

        simulator.simulateShardStarted(relocationShards.v2(), true);

        var result = simulator.getSimulatedHeapMetrics();
        // nodeA: non-last shard removed → no index heap delta; only shard heap delta = -100
        // totalHeap: 200 - 100 = 100 (not clamped)
        assertThat(result.get(nodeA).nodeHeapEstimates().totalHeapUsage(), equalTo(100L));
        // hostedShardsHeap: 30 - 100 = -70 → clamped to 0
        assertThat(result.get(nodeA).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(0L));
    }

    /**
     * Non-indexing nodes (nodes without the {@link DiscoveryNodeRole#INDEX_ROLE}, such as stateless search nodes) always have
     * totalHeapUsage reported as zero when a delta is present, regardless of the delta magnitude.
     * hostedShardsHeapUsage is still computed with the normal clamped adjustment.
     * In contrast, indexing nodes have their totalHeapUsage updated by the delta as normal.
     * This is verified via both the simulateAddIndexToNode and simulateShardStarted paths.
     */
    public void testNonIndexingNodeTotalHeapEstimateIsZeroWhenDeltaIsPresent() {
        var searchNodeId = "search-node";
        var indexingNodeId = "indexing-node";
        long indexingInitialTotal = randomLongBetween(200, 500);
        long indexingInitialHosted = randomLongBetween(50, 150);
        long searchInitialHosted = randomLongBetween(50, 150);
        long shardHeap = randomLongBetween(10, 50);
        long indexHeap = randomLongBetween(10, 40);
        var indexMetadata = IndexMetadata.builder("test-index").settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        var index = indexMetadata.getIndex();
        var shardId0 = new ShardId(index, 0);
        var initialMetrics = Map.of(
            searchNodeId,
            nodeHeapMetrics(searchNodeId, 0, searchInitialHosted),
            indexingNodeId,
            nodeHeapMetrics(indexingNodeId, indexingInitialTotal, indexingInitialHosted)
        );
        var shardHeapUsages = Map.of(shardId0, new ShardAndIndexHeapUsage(shardHeap, indexHeap));

        // Both branches produce expectedDelta = shardHeap + indexHeap per node, via different call paths.
        NodeHeapMemoryShardMovementSimulator simulator;

        var irtBuilder = IndexRoutingTable.builder(index);
        final var unassignedPrimary = newShardRouting(shardId0, null, true, UNASSIGNED);
        final var unassignedReplica = newShardRouting(shardId0, null, false, UNASSIGNED);
        irtBuilder.addShard(unassignedPrimary);
        irtBuilder.addShard(unassignedReplica);
        final var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(createDiscoveryNodes(Set.of(indexingNodeId), Set.of(searchNodeId)))
            .metadata(Metadata.builder().put(ProjectMetadata.builder(ProjectId.DEFAULT).put(indexMetadata, false)).build())
            .routingTable(
                GlobalRoutingTable.builder().put(ProjectId.DEFAULT, RoutingTable.builder().add(irtBuilder.build()).build()).build()
            )
            .build();
        final var routingNodes = state.mutableRoutingNodes();
        final var initializingPrimary = routingNodes.initializeShard(
            unassignedPrimary,
            indexingNodeId,
            null,
            0L,
            RoutingChangesObserver.NOOP
        );
        final var startedPrimary = routingNodes.startShard(initializingPrimary, RoutingChangesObserver.NOOP, 0L);
        final var initializingReplica = routingNodes.initializeShard(
            unassignedReplica,
            searchNodeId,
            null,
            0L,
            RoutingChangesObserver.NOOP
        );
        final var startedReplica = routingNodes.startShard(initializingReplica, RoutingChangesObserver.NOOP, 0L);

        if (randomBoolean()) {
            // add shard and index usage separately
            simulator = newSimulator(initialMetrics, shardHeapUsages, ShardAndIndexHeapUsage.ZERO, routingNodes);
            simulator.simulateShardStarted(startedPrimary, false);
            simulator.simulateShardStarted(startedReplica, false);
            simulator.simulateAddIndexToNode(searchNodeId, index);
            simulator.simulateAddIndexToNode(indexingNodeId, index);
        } else {
            // or add shard and index usage together
            simulator = newSimulator(initialMetrics, shardHeapUsages, ShardAndIndexHeapUsage.ZERO, routingNodes);
            simulator.simulateShardStarted(startedPrimary, true);
            simulator.simulateShardStarted(startedReplica, true);
        }

        long expectedDelta = shardHeap + indexHeap;
        var result = simulator.getSimulatedHeapMetrics();
        assertThat(result.get(searchNodeId).nodeHeapEstimates().totalHeapUsage(), equalTo(0L));
        assertThat(result.get(searchNodeId).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(searchInitialHosted + expectedDelta));
        assertThat(result.get(indexingNodeId).nodeHeapEstimates().totalHeapUsage(), equalTo(indexingInitialTotal + expectedDelta));
        assertThat(result.get(indexingNodeId).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(indexingInitialHosted + expectedDelta));
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

        simulator.simulateShardStarted(relocationShards.v2(), true);

        assertThat(simulator.getSimulatedHeapMetrics().size(), equalTo(0));
    }

    /** simulateAddIndexToNode increases totalHeapUsage and hostedShardsHeapUsage by the index heap amount. */
    public void testSimulateAddIndexToNodeIncrementsTotalAndHostedShardsHeap() {
        var nodeId = "node-0";
        long shardHeap = randomLongBetween(100, 150), indexHeap = randomLongBetween(30, 50);
        long initialTotal = randomLongBetween(500, 1000), initialHosted = randomLongBetween(300, 500);
        var index = new Index("test-index", "_na_");

        var simulator = newSimulator(
            Map.of(nodeId, nodeHeapMetrics(nodeId, initialTotal, initialHosted)),
            Map.of(new ShardId(index, 0), new ShardAndIndexHeapUsage(shardHeap, indexHeap)),
            ShardAndIndexHeapUsage.ZERO,
            routingNodes(nodeId)
        );

        simulator.simulateAddIndexToNode(nodeId, index);

        var result = simulator.getSimulatedHeapMetrics();
        assertThat(result.get(nodeId).nodeHeapEstimates().totalHeapUsage(), equalTo(initialTotal + indexHeap));
        assertThat(result.get(nodeId).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(initialHosted + indexHeap));
    }

    /** simulateRemoveIndexFromNode decreases totalHeapUsage and hostedShardsHeapUsage by the index heap amount. */
    public void testSimulateRemoveIndexFromNodeDecrementsTotalAndHostedShardsHeap() {
        var nodeId = "node-0";
        long shardHeap = randomLongBetween(100, 150), indexHeap = randomLongBetween(30, 50);
        long initialTotal = randomLongBetween(500, 1000), initialHosted = randomLongBetween(300, 500);
        var index = new Index("test-index", "_na_");

        var simulator = newSimulator(
            Map.of(nodeId, nodeHeapMetrics(nodeId, initialTotal, initialHosted)),
            Map.of(new ShardId(index, 0), new ShardAndIndexHeapUsage(shardHeap, indexHeap)),
            ShardAndIndexHeapUsage.ZERO,
            routingNodes(nodeId)
        );

        simulator.simulateRemoveIndexFromNode(nodeId, index);

        var result = simulator.getSimulatedHeapMetrics();
        assertThat(result.get(nodeId).nodeHeapEstimates().totalHeapUsage(), equalTo(initialTotal - indexHeap));
        assertThat(result.get(nodeId).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(initialHosted - indexHeap));
    }

    /** simulateAddIndexToNode is a no-op for nodes absent from the initial metrics map. */
    public void testSimulateAddIndexToNodeSkipsUnknownNodes() {
        var index = new Index("test-index", "_na_");
        var simulator = newSimulator(Map.of(), Map.of(), ShardAndIndexHeapUsage.ZERO, routingNodes());

        final var nodeId = "unknown-node";
        simulator.simulateAddIndexToNode(nodeId, index);

        assertThat(simulator.getSimulatedHeapMetrics().size(), equalTo(0));
    }

    /** simulateRemoveIndexFromNode is a no-op for nodes absent from the initial metrics map. */
    public void testSimulateRemoveIndexFromNodeSkipsUnknownNodes() {
        var index = new Index("test-index", "_na_");
        var simulator = newSimulator(Map.of(), Map.of(), ShardAndIndexHeapUsage.ZERO, routingNodes());

        final var nodeId = "unknown-node";
        simulator.simulateRemoveIndexFromNode(nodeId, index);

        assertThat(simulator.getSimulatedHeapMetrics().size(), equalTo(0));
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

    private static RoutingNodes routingNodes(String... indexingNodes) {
        return RoutingNodes.immutable(GlobalRoutingTable.EMPTY_ROUTING_TABLE, createDiscoveryNodes(indexingNodes));
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
            .nodes(createDiscoveryNodes(primaryNode, otherNode))
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

    private static DiscoveryNodes createDiscoveryNodes(String... indexingNodeIds) {
        return createDiscoveryNodes(Set.of(indexingNodeIds), Set.of());
    }

    private static DiscoveryNodes createDiscoveryNodes(Set<String> indexingNodeIds, Set<String> searchNodeIds) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (String nodeId : indexingNodeIds) {
            builder.add(newNode(nodeId, Set.of(DiscoveryNodeRole.INDEX_ROLE)));
        }
        for (String nodeId : searchNodeIds) {
            builder.add(newNode(nodeId, Set.of(DiscoveryNodeRole.SEARCH_ROLE)));
        }
        return builder.build();
    }
}
