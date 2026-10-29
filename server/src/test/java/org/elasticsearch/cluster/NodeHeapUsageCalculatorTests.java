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
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;

public class NodeHeapUsageCalculatorTests extends ESTestCase {

    public void testHostedShardsIncludesIndexHeapAndNodeLocalPostings() {
        final ShardId nodeAShard = new ShardId(new Index("index-a", "uuid-a"), 0);
        final ShardId nodeBShard = new ShardId(new Index("index-b", "uuid-b"), 0);
        final long nonShardHeapUsage = 1_000L;
        final ClusterState clusterState = clusterStateWithStartedShards(
            Map.of("node-a", DiscoveryNodeRole.INDEX_ROLE, "node-b", DiscoveryNodeRole.INDEX_ROLE),
            Map.of(nodeAShard, "node-a", nodeBShard, "node-b")
        );

        final var result = NodeHeapUsageCalculator.calculateForRoutingNodes(
            clusterState,
            nonShardHeapUsage,
            new ShardHeapUsageEstimates(
                Map.of(nodeAShard, new ShardAndIndexHeapUsage(10L, 100L, 5L), nodeBShard, new ShardAndIndexHeapUsage(20L, 200L, 17L)),
                ShardAndIndexHeapUsage.ZERO
            )
        );

        // Each ShardAndIndexHeapUsage is (shard heap, index heap, postings). The postings are already included in the shard heap.
        // Node A is (10, 100, 5): 10 bytes of shard heap, of which 5 are postings, plus 100 bytes of index heap.
        // Node B is (20, 200, 17): 20 bytes of shard heap, of which 17 are postings, plus 200 bytes of index heap.
        //
        // Hosted-shards heap keeps whatever postings that node actually has:
        // node A = 10 + 100 = 110
        // node B = 20 + 200 = 220
        //
        // The indexing-node total does not use each node's own postings. It uses the larger of the two, which is node B's 17.
        // The local postings are taken back out of the shard heap first, so they are not counted twice:
        // node A = 1000 non-shard + (10 - 5) shard + 100 index + 17 max postings = 1122
        // node B = 1000 non-shard + (20 - 17) shard + 200 index + 17 max postings = 1220
        // Node B's total is 1000 + 220 because its own postings are already the maximum.
        assertThat(result.maxPostingsHeapUsage(), equalTo(17L));
        assertThat(result.nodeHeapEstimates().get("node-a"), equalTo(new NodeHeapEstimates(1_122L, 110L, nonShardHeapUsage)));
        assertThat(result.nodeHeapEstimates().get("node-b"), equalTo(new NodeHeapEstimates(1_220L, 220L, nonShardHeapUsage)));
    }

    public void testIndexHeapIsCountedOncePerNode() {
        final Index index = new Index("index", "uuid");
        final ShardId shard0 = new ShardId(index, 0);
        final ShardId shard1 = new ShardId(index, 1);
        final long nonShardHeapUsage = 50L;

        final var result = NodeHeapUsageCalculator.calculateForShardEstimates(
            nonShardHeapUsage,
            new ShardHeapUsageEstimates(
                Map.of(shard0, new ShardAndIndexHeapUsage(10L, 100L, 5L), shard1, new ShardAndIndexHeapUsage(20L, 100L, 7L)),
                ShardAndIndexHeapUsage.ZERO
            )
        );

        // Both shards are from the same index, so the 100 byte index heap is counted once. A one-node calculation uses local postings,
        // which are already inside shardHeapUsageBytes, so total = non-shard + hosted.
        // hosted = 10 + 20 + 100 = 130; total = 50 + 130 = 180.
        assertThat(result.totalHeapUsage(), equalTo(180L));
        assertThat(result.hostedShardsHeapUsage(), equalTo(130L));
        assertThat(result.nonShardHeapUsage(), equalTo(nonShardHeapUsage));
    }

    public void testRoutingNodeCalculationIgnoresInactiveShards() {
        final ShardId shard = new ShardId(new Index("index", "uuid"), 0);
        final long nonShardHeapUsage = 50L;
        final ClusterState clusterState = clusterStateWithShards(
            Map.of("node", DiscoveryNodeRole.INDEX_ROLE),
            Map.of(shard, "node"),
            ShardRoutingState.INITIALIZING
        );

        final var result = NodeHeapUsageCalculator.calculateForRoutingNodes(
            clusterState,
            nonShardHeapUsage,
            new ShardHeapUsageEstimates(Map.of(shard, new ShardAndIndexHeapUsage(10L, 100L, 5L)), ShardAndIndexHeapUsage.ZERO)
        );

        assertThat(result.maxPostingsHeapUsage(), equalTo(0L));
        assertThat(result.nodeHeapEstimates().get("node"), equalTo(new NodeHeapEstimates(nonShardHeapUsage, 0L, nonShardHeapUsage)));
    }

    public void testIndexHeapIsCountedOncePerIndexIdentity() {
        final Index index1 = new Index("same-name", "uuid-1");
        final Index index2 = new Index("same-name", "uuid-2");
        final ShardId shard1 = new ShardId(index1, 0);
        final ShardId shard2 = new ShardId(index2, 0);
        final long nonShardHeapUsage = 50L;
        final ClusterState clusterState = multiProjectClusterStateWithStartedShardsOnSameNode("node", shard1, shard2);

        final var result = NodeHeapUsageCalculator.calculateForRoutingNodes(
            clusterState,
            nonShardHeapUsage,
            new ShardHeapUsageEstimates(
                Map.of(shard1, new ShardAndIndexHeapUsage(10L, 100L, 5L), shard2, new ShardAndIndexHeapUsage(20L, 1_000L, 7L)),
                ShardAndIndexHeapUsage.ZERO
            )
        );

        // These indices share a name but not a UUID, so each incurs its own index heap.
        // hosted = 10 + 20 + 100 + 1000 = 1130; total = 50 + 1130 = 1180.
        assertThat(result.maxPostingsHeapUsage(), equalTo(12L));
        assertThat(result.nodeHeapEstimates().get("node"), equalTo(new NodeHeapEstimates(1_180L, 1_130L, nonShardHeapUsage)));
    }

    public void testTotalHeapCanBeLeftUnmodeledForSearchNodes() {
        final ShardId indexNodeShard = new ShardId(new Index("index-node-index", "index-node-uuid"), 0);
        final ShardId searchNodeShard = new ShardId(new Index("search-node-index", "search-node-uuid"), 0);
        final long nonShardHeapUsage = 50L;
        final ClusterState clusterState = clusterStateWithStartedShards(
            Map.of("index-node", DiscoveryNodeRole.INDEX_ROLE, "search-node", DiscoveryNodeRole.SEARCH_ROLE),
            Map.of(indexNodeShard, "index-node", searchNodeShard, "search-node")
        );

        final var result = NodeHeapUsageCalculator.calculateForRoutingNodes(
            clusterState,
            nonShardHeapUsage,
            new ShardHeapUsageEstimates(
                Map.of(
                    indexNodeShard,
                    new ShardAndIndexHeapUsage(10L, 100L, 5L),
                    searchNodeShard,
                    new ShardAndIndexHeapUsage(1_200L, 200L, 1_000L)
                ),
                ShardAndIndexHeapUsage.ZERO
            )
        );

        // The search node's postings are larger, but they do not feed the max used for modeled totals, and its total stays unmodeled.
        assertThat(result.maxPostingsHeapUsage(), equalTo(5L));
        // hosted = 10 + 100 = 110; total = 50 + (10 - 5) + 100 + 5 = 160
        assertThat(result.nodeHeapEstimates().get("index-node"), equalTo(new NodeHeapEstimates(160L, 110L, nonShardHeapUsage)));
        // hosted = 1200 + 200 = 1400; total and non-shard stay 0
        assertThat(result.nodeHeapEstimates().get("search-node"), equalTo(new NodeHeapEstimates(0L, 1_400L, 0L)));
    }

    public void testDefaultShardHeapUsageIsUsedForShardWithoutMetrics() {
        final ShardId shard = new ShardId(new Index("index", "uuid"), 0);
        final long nonShardHeapUsage = 50L;
        final ClusterState clusterState = clusterStateWithStartedShards(
            Map.of("node", DiscoveryNodeRole.INDEX_ROLE),
            Map.of(shard, "node")
        );

        final var result = NodeHeapUsageCalculator.calculateForRoutingNodes(
            clusterState,
            nonShardHeapUsage,
            new ShardHeapUsageEstimates(Map.of(), new ShardAndIndexHeapUsage(10L, 100L, 5L))
        );

        // hosted = 10 + 100 = 110; total = 50 + (10 - 5) + 100 + 5 = 160
        assertThat(result.maxPostingsHeapUsage(), equalTo(5L));
        assertThat(result.nodeHeapEstimates().get("node"), equalTo(new NodeHeapEstimates(160L, 110L, nonShardHeapUsage)));
    }

    private static ClusterState clusterStateWithStartedShards(
        Map<String, DiscoveryNodeRole> nodeRoles,
        Map<ShardId, String> currentNodeByShard
    ) {
        return clusterStateWithShards(nodeRoles, currentNodeByShard, ShardRoutingState.STARTED);
    }

    private static ClusterState clusterStateWithShards(
        Map<String, DiscoveryNodeRole> nodeRoles,
        Map<ShardId, String> currentNodeByShard,
        ShardRoutingState shardRoutingState
    ) {
        final var nodes = DiscoveryNodes.builder();
        nodeRoles.forEach((nodeId, role) -> nodes.add(DiscoveryNodeUtils.builder(nodeId).roles(Set.of(role)).build()));

        final var highestShardIdByIndex = new HashMap<Index, Integer>();
        currentNodeByShard.keySet().forEach(shardId -> highestShardIdByIndex.merge(shardId.getIndex(), shardId.id(), Math::max));
        final var metadata = Metadata.builder();
        highestShardIdByIndex.forEach(
            (index, highestShardId) -> metadata.put(
                IndexMetadata.builder(index.getName())
                    .settings(indexSettings(IndexVersion.current(), index.getUUID(), highestShardId + 1, 0))
                    .build(),
                false
            )
        );

        final var routingByIndex = new HashMap<Index, IndexRoutingTable.Builder>();
        currentNodeByShard.forEach(
            (shardId, nodeId) -> routingByIndex.computeIfAbsent(shardId.getIndex(), IndexRoutingTable::builder)
                .addShard(newShardRouting(shardId, nodeId, true, shardRoutingState))
        );
        final var routingTable = RoutingTable.builder();
        routingByIndex.values().forEach(routingTable::add);
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(metadata).routingTable(routingTable).build();
    }

    private static ClusterState multiProjectClusterStateWithStartedShardsOnSameNode(String nodeId, ShardId shard1, ShardId shard2) {
        final var project1 = projectMetadata(ProjectId.fromId("project-1"), shard1.getIndex());
        final var project2 = projectMetadata(ProjectId.fromId("project-2"), shard2.getIndex());
        final var nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder(nodeId).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build());
        final var routingTable1 = RoutingTable.builder()
            .add(IndexRoutingTable.builder(shard1.getIndex()).addShard(newShardRouting(shard1, nodeId, true, ShardRoutingState.STARTED)))
            .build();
        final var routingTable2 = RoutingTable.builder()
            .add(IndexRoutingTable.builder(shard2.getIndex()).addShard(newShardRouting(shard2, nodeId, true, ShardRoutingState.STARTED)))
            .build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes)
            .metadata(Metadata.builder().put(project1).put(project2).build())
            .routingTable(GlobalRoutingTable.builder().put(project1.id(), routingTable1).put(project2.id(), routingTable2).build())
            .build();
    }

    private static ProjectMetadata projectMetadata(ProjectId projectId, Index index) {
        return ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(index.getName()).settings(indexSettings(IndexVersion.current(), index.getUUID(), 1, 0)).build(),
                false
            )
            .build();
    }
}
