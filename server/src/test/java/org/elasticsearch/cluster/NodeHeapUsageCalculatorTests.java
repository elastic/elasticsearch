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

        // Total heap uses the largest postings value across estimated nodes, so node-a uses node-b's 17 byte postings value:
        // node-a total = 1000 non-shard + 10 shard + 100 index + 17 max postings = 1127.
        // Hosted-shards uses node-local postings: 10 shard + 100 index + 5 postings = 115.
        assertThat(result.maxPostingsHeapUsage(), equalTo(17L));
        assertThat(result.nodeHeapEstimates().get("node-a"), equalTo(new NodeHeapEstimates(1_127L, 115L, nonShardHeapUsage)));
        assertThat(result.nodeHeapEstimates().get("node-b"), equalTo(new NodeHeapEstimates(1_237L, 237L, nonShardHeapUsage)));
    }

    public void testIndexHeapIsCountedOncePerNode() {
        final Index index = new Index("index", "uuid");
        final ShardId shard0 = new ShardId(index, 0);
        final ShardId shard1 = new ShardId(index, 1);
        var nonShardHeapUsage = 50L;
        final ClusterState clusterState = clusterStateWithStartedShards(
            Map.of("node", DiscoveryNodeRole.INDEX_ROLE),
            Map.of(shard0, "node", shard1, "node")
        );

        final var result = NodeHeapUsageCalculator.calculateForRoutingNode(
            clusterState.getRoutingNodes().node("node"),
            nonShardHeapUsage,
            new ShardHeapUsageEstimates(
                Map.of(shard0, new ShardAndIndexHeapUsage(10L, 100L, 5L), shard1, new ShardAndIndexHeapUsage(20L, 100L, 7L)),
                ShardAndIndexHeapUsage.ZERO
            )
        );

        // Both shards are from the same index, so the 100 byte index heap is counted once:
        // total = 50 non-shard + 30 shard + 100 index + 12 postings; hosted = 30 shard + 100 index + 12 postings.
        assertThat(result.totalHeapUsage(), equalTo(192L));
        assertThat(result.hostedShardsHeapUsage(), equalTo(142L));
        assertThat(result.nonShardHeapUsage(), equalTo(50L));
    }

    public void testIndexHeapIsCountedOncePerIndexIdentity() {
        final Index index1 = new Index("same-name", "uuid-1");
        final Index index2 = new Index("same-name", "uuid-2");
        final ShardId shard1 = new ShardId(index1, 0);
        final ShardId shard2 = new ShardId(index2, 0);
        final long nonShardHeapUsage = 50L;
        final long shard1HeapUsage = 10L;
        final long shard2HeapUsage = 20L;
        final long index1OverheadUsage = 100L;
        final long index2OverheadUsage = 1_000L;
        final long shard1PostingsUsage = 5L;
        final long shard2PostingsUsage = 7L;
        final ClusterState clusterState = multiProjectClusterStateWithStartedShardsOnSameNode("node", shard1, shard2);

        final var result = NodeHeapUsageCalculator.calculateForRoutingNodes(
            clusterState,
            nonShardHeapUsage,
            new ShardHeapUsageEstimates(
                Map.of(
                    shard1,
                    new ShardAndIndexHeapUsage(shard1HeapUsage, index1OverheadUsage, shard1PostingsUsage),
                    shard2,
                    new ShardAndIndexHeapUsage(shard2HeapUsage, index2OverheadUsage, shard2PostingsUsage)
                ),
                ShardAndIndexHeapUsage.ZERO
            )
        );

        // These indices have the same name but different UUIDs, so they are separate Index identities and each incurs index heap.
        // There is one index node, so max postings is the same as its local postings:
        // total = 50 non-shard + 30 shard + 1100 index overhead + 12 postings; hosted = 30 shard + 1100 index overhead + 12 postings.
        assertThat(result.maxPostingsHeapUsage(), equalTo(12L));
        assertThat(result.nodeHeapEstimates().get("node"), equalTo(new NodeHeapEstimates(1_192L, 1_142L, nonShardHeapUsage)));
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
                    new ShardAndIndexHeapUsage(20L, 200L, 1_000L)
                ),
                ShardAndIndexHeapUsage.ZERO
            )
        );

        // The search node's postings are larger, but its total is not modeled. Only index-node postings feed the max value used for
        // modeled totals.
        assertThat(result.maxPostingsHeapUsage(), equalTo(5L));
        // Index node: total = 50 non-shard + 10 shard + 100 index + 5 max postings; hosted = 10 + 100 + 5 local postings.
        assertThat(result.nodeHeapEstimates().get("index-node"), equalTo(new NodeHeapEstimates(165L, 115L, nonShardHeapUsage)));
        // Search node: total and non-shard heap remain unmodeled as 0, while hosted-shards still reports
        // 20 shard + 200 index + 1000 local postings.
        assertThat(result.nodeHeapEstimates().get("search-node"), equalTo(new NodeHeapEstimates(0L, 1_220L, 0L)));
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

        // No per-shard metric exists, so the default estimate supplies all components:
        // total = 50 non-shard + 10 shard + 100 index + 5 max postings; hosted = 10 shard + 100 index + 5 local postings.
        assertThat(result.nodeHeapEstimates().get("node"), equalTo(new NodeHeapEstimates(165L, 115L, nonShardHeapUsage)));
    }

    private static ClusterState clusterStateWithStartedShards(
        Map<String, DiscoveryNodeRole> nodeRoles,
        Map<ShardId, String> currentNodeByShard
    ) {
        final var nodes = DiscoveryNodes.builder();
        nodeRoles.forEach((nodeId, role) -> nodes.add(DiscoveryNodeUtils.builder(nodeId).roles(Set.of(role)).build()));

        final var highestShardIdByIndex = new HashMap<Index, Integer>();
        currentNodeByShard.keySet().forEach(shardId -> highestShardIdByIndex.merge(shardId.getIndex(), shardId.id(), Math::max));
        final var metadata = Metadata.builder();
        highestShardIdByIndex.forEach(
            (index, highestShardId) -> metadata.put(
                IndexMetadata.builder(index.getName()).settings(indexSettings(IndexVersion.current(), highestShardId + 1, 0)).build(),
                false
            )
        );

        final var routingByIndex = new HashMap<Index, IndexRoutingTable.Builder>();
        currentNodeByShard.forEach(
            (shardId, nodeId) -> routingByIndex.computeIfAbsent(shardId.getIndex(), IndexRoutingTable::builder)
                .addShard(newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED))
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
                IndexMetadata.builder(index.getName())
                    .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID()))
                    .build(),
                false
            )
            .build();
    }
}
