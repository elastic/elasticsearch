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
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class NodeHeapMemoryShardMovementSimulatorTests extends ESAllocationTestCase {

    private static final long TOTAL_HEAP_BYTES = 2000L;

    /** No deltas → returns the same initial map reference (fast path). */
    public void testNoDeltasReturnsSameMetricsReference() {
        var nodeId = "node-0";
        var initialMetrics = Map.of(nodeId, nodeHeapMetrics(nodeId, randomIntBetween(100, 300), randomIntBetween(0, 100)));
        var simulator = newSimulator(initialMetrics, Map.of(), ShardAndIndexHeapUsage.ZERO, emptyRoutingNodes());

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
        var relocationShards = routingNodes.relocateShard(startedShard, nodeB, 0, "test", RoutingChangesObserver.NOOP);

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
        // nodeA: remove shardHeap; initial hosted=30 → max(0, 30 - shardHeap) = 0
        assertThat(result.get(nodeA).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(0L));
        // nodeB: add shardHeap + indexHeap = shardHeap + indexHeap; initial total=0 → shardHeap + indexHeap
        assertThat(result.get(nodeB).nodeHeapEstimates().totalHeapUsage(), equalTo(shardHeap + indexHeap));
        // nodeB: add shardHeap; initial hosted=0 → shardHeap
        assertThat(result.get(nodeB).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(shardHeap));
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
        var relocationShards = routingNodes.relocateShard(shard0, nodeB, 0, "test", RoutingChangesObserver.NOOP);

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

    /** Nodes not present in the initial metrics map are silently skipped; results for known nodes are unaffected. */
    public void testNodeWithoutInitialMetricsIsSkipped() {
        var nodeA = "node-a";
        var nodeB = "node-b";
        var state = buildSingleShardState("test-index", nodeA, nodeB);
        var routingNodes = state.mutableRoutingNodes();
        var startedShard = getSoleStartedShard(routingNodes, nodeA);
        var relocationShards = routingNodes.relocateShard(startedShard, nodeB, 0, "test", RoutingChangesObserver.NOOP);

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

    /** simulateAddIndexToNode increases totalHeapUsage by the index heap amount and does not affect hostedShardsHeapUsage. */
    public void testSimulateAddIndexToNodeIncrementsTotalHeapOnly() {
        var nodeId = "node-0";
        long shardHeap = randomLongBetween(100, 150), indexHeap = randomLongBetween(30, 50);
        long initialTotal = randomLongBetween(500, 1000), initialHosted = randomLongBetween(300, 500);
        var index = new Index("test-index", "_na_");

        var simulator = newSimulator(
            Map.of(nodeId, nodeHeapMetrics(nodeId, initialTotal, initialHosted)),
            Map.of(new ShardId(index, 0), new ShardAndIndexHeapUsage(shardHeap, indexHeap)),
            ShardAndIndexHeapUsage.ZERO,
            emptyRoutingNodes()
        );

        simulator.simulateAddIndexToNode(nodeId, index);

        var result = simulator.getSimulatedHeapMetrics();
        assertThat(result.get(nodeId).nodeHeapEstimates().totalHeapUsage(), equalTo(initialTotal + indexHeap));
        assertThat(result.get(nodeId).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(initialHosted));
    }

    /** simulateRemoveIndexFromNode decreases totalHeapUsage by the index heap amount and does not affect hostedShardsHeapUsage. */
    public void testSimulateRemoveIndexFromNodeDecrementsTotalHeapOnly() {
        var nodeId = "node-0";
        long shardHeap = randomLongBetween(100, 150), indexHeap = randomLongBetween(30, 50);
        long initialTotal = randomLongBetween(500, 1000), initialHosted = randomLongBetween(300, 500);
        var index = new Index("test-index", "_na_");

        var simulator = newSimulator(
            Map.of(nodeId, nodeHeapMetrics(nodeId, initialTotal, initialHosted)),
            Map.of(new ShardId(index, 0), new ShardAndIndexHeapUsage(shardHeap, indexHeap)),
            ShardAndIndexHeapUsage.ZERO,
            emptyRoutingNodes()
        );

        simulator.simulateRemoveIndexFromNode(nodeId, index);

        var result = simulator.getSimulatedHeapMetrics();
        assertThat(result.get(nodeId).nodeHeapEstimates().totalHeapUsage(), equalTo(initialTotal - indexHeap));
        assertThat(result.get(nodeId).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(initialHosted));
    }

    /** simulateAddIndexToNode is a no-op for nodes absent from the initial metrics map. */
    public void testSimulateAddIndexToNodeSkipsUnknownNodes() {
        var index = new Index("test-index", "_na_");
        var simulator = newSimulator(Map.of(), Map.of(), ShardAndIndexHeapUsage.ZERO, emptyRoutingNodes());

        simulator.simulateAddIndexToNode("unknown-node", index);

        assertThat(simulator.getSimulatedHeapMetrics().size(), equalTo(0));
    }

    /** simulateRemoveIndexFromNode is a no-op for nodes absent from the initial metrics map. */
    public void testSimulateRemoveIndexFromNodeSkipsUnknownNodes() {
        var index = new Index("test-index", "_na_");
        var simulator = newSimulator(Map.of(), Map.of(), ShardAndIndexHeapUsage.ZERO, emptyRoutingNodes());

        simulator.simulateRemoveIndexFromNode("unknown-node", index);

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

    private static RoutingNodes emptyRoutingNodes() {
        return RoutingNodes.immutable(GlobalRoutingTable.EMPTY_ROUTING_TABLE, DiscoveryNodes.EMPTY_NODES);
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
