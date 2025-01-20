/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datatiers;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.datatiers.DataTierUsageFixtures.buildNodeIndicesStats;
import static org.elasticsearch.xpack.core.datatiers.DataTierUsageFixtures.indexMetadata;
import static org.elasticsearch.xpack.core.datatiers.DataTierUsageFixtures.newNode;
import static org.elasticsearch.xpack.core.datatiers.DataTierUsageFixtures.routeTestShardToNodes;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class NodesDataTiersUsageTransportActionTests extends ESTestCase {

    private long byteSize;
    private long docCount;

    @Before
    public void setup() {
        byteSize = randomLongBetween(1024L, 1024L * 1024L * 1024L * 30L); // 1 KB to 30 GB
        docCount = randomLongBetween(100L, 100000000L); // one hundred to one hundred million
    }

    public void testCalculateStatsNoTiers() {
        // Nodes: 0 Tiered Nodes, 1 Data Node
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        DiscoveryNode dataNode1 = newNode(1, DiscoveryNodeRole.DATA_ROLE);
        discoBuilder.add(dataNode1);
        discoBuilder.localNodeId(dataNode1.getId());

        // Indices: 1 Regular index
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        IndexMetadata index1 = indexMetadata("index_1", 3, 1);
        metadataBuilder.put(index1, false).generateClusterUuidIfNeeded();

        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index1.getIndex());
        routeTestShardToNodes(index1, 0, indexRoutingTableBuilder, dataNode1);
        routeTestShardToNodes(index1, 1, indexRoutingTableBuilder, dataNode1);
        routeTestShardToNodes(index1, 2, indexRoutingTableBuilder, dataNode1);
        routingTableBuilder.add(indexRoutingTableBuilder.build());

        // Cluster State and create stats responses
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(metadataBuilder)
            .nodes(discoBuilder)
            .routingTable(routingTableBuilder.build())
            .build();
        NodeIndicesStats nodeIndicesStats = buildNodeIndicesStats(
            clusterState.getRoutingNodes().node(dataNode1.getId()),
            byteSize,
            docCount
        );

        // Calculate usage
        Map<String, NodeDataTiersUsage.UsageStats> usageStats = NodesDataTiersUsageTransportAction.aggregateStats(
            clusterState.getRoutingNodes().node(dataNode1.getId()),
            clusterState.metadata(),
            nodeIndicesStats
        );

        // Verify - No results when no tiers present
        assertThat(usageStats.size(), is(0));
    }

    public void testCalculateStatsNoIndices() {
        // Nodes: 1 Data, 1 Hot, 1 Warm, 1 Cold, 1 Frozen
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        DiscoveryNode dataNode1 = newNode(1, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        discoBuilder.add(dataNode1);
        discoBuilder.localNodeId(dataNode1.getId());

        // Indices: 1 Regular index, not hosted on any tiers
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        // Cluster State and create stats responses
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(metadataBuilder)
            .nodes(discoBuilder)
            .routingTable(routingTableBuilder.build())
            .build();
        NodeIndicesStats nodeIndicesStats = buildNodeIndicesStats(
            clusterState.getRoutingNodes().node(dataNode1.getId()),
            byteSize,
            docCount
        );

        // Calculate usage
        Map<String, NodeDataTiersUsage.UsageStats> usageStats = NodesDataTiersUsageTransportAction.aggregateStats(
            clusterState.getRoutingNodes().node(dataNode1.getId()),
            clusterState.metadata(),
            nodeIndicesStats
        );

        // Verify - No results when no tiers present
        assertThat(usageStats.size(), is(0));
    }

    public void testCalculateStatsTieredIndicesOnly() {
        // Nodes: 3 Data, 0 Tiered - Only hosting indices on generic data nodes
        int nodeId = 0;
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();

        DiscoveryNode dataNode1 = newNode(nodeId++, DiscoveryNodeRole.DATA_ROLE);
        discoBuilder.add(dataNode1);
        DiscoveryNode dataNode2 = newNode(nodeId, DiscoveryNodeRole.DATA_ROLE);
        discoBuilder.add(dataNode2);

        discoBuilder.localNodeId(dataNode1.getId());

        // Indices: 1 Hot index, 2 Warm indices, 3 Cold indices
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        IndexMetadata hotIndex1 = indexMetadata("hot_index_1", 3, 1, DataTier.DATA_HOT);
        metadataBuilder.put(hotIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(hotIndex1.getIndex());
            routeTestShardToNodes(hotIndex1, 0, indexRoutingTableBuilder, dataNode1, dataNode2);
            routeTestShardToNodes(hotIndex1, 1, indexRoutingTableBuilder, dataNode2, dataNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        IndexMetadata warmIndex1 = indexMetadata("warm_index_1", 1, 1, DataTier.DATA_WARM);
        metadataBuilder.put(warmIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(warmIndex1.getIndex());
            routeTestShardToNodes(warmIndex1, 0, indexRoutingTableBuilder, dataNode1, dataNode2);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        IndexMetadata warmIndex2 = indexMetadata("warm_index_2", 1, 1, DataTier.DATA_WARM);
        metadataBuilder.put(warmIndex2, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(warmIndex2.getIndex());
            routeTestShardToNodes(warmIndex2, 0, indexRoutingTableBuilder, dataNode2, dataNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        IndexMetadata coldIndex1 = indexMetadata("cold_index_1", 1, 0, DataTier.DATA_COLD);
        metadataBuilder.put(coldIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(coldIndex1.getIndex());
            routeTestShardToNodes(coldIndex1, 0, indexRoutingTableBuilder, dataNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        // Cluster State and create stats responses
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();
        NodeIndicesStats nodeIndicesStats = buildNodeIndicesStats(
            clusterState.getRoutingNodes().node(dataNode1.getId()),
            byteSize,
            docCount
        );

        // Calculate usage
        Map<String, NodeDataTiersUsage.UsageStats> usageStats = NodesDataTiersUsageTransportAction.aggregateStats(
            clusterState.getRoutingNodes().node(dataNode1.getId()),
            clusterState.metadata(),
            nodeIndicesStats
        );

        // Verify - Index stats exist for the tiers, but no tiered nodes are found
        assertThat(usageStats.size(), is(3));

        NodeDataTiersUsage.UsageStats hotStats = usageStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.getPrimaryShardSizes(), equalTo(List.of(byteSize)));
        assertThat(hotStats.getTotalShardCount(), is(2));
        assertThat(hotStats.getDocCount(), is(hotStats.getTotalShardCount() * docCount));
        assertThat(hotStats.getTotalSize(), is(hotStats.getTotalShardCount() * byteSize));

        NodeDataTiersUsage.UsageStats warmStats = usageStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.getPrimaryShardSizes(), equalTo(List.of(byteSize)));
        assertThat(warmStats.getTotalShardCount(), is(2));
        assertThat(warmStats.getDocCount(), is(warmStats.getTotalShardCount() * docCount));
        assertThat(warmStats.getTotalSize(), is(warmStats.getTotalShardCount() * byteSize));

        NodeDataTiersUsage.UsageStats coldStats = usageStats.get(DataTier.DATA_COLD);
        assertThat(coldStats, is(notNullValue()));
        assertThat(coldStats.getPrimaryShardSizes(), equalTo(List.of(byteSize)));
        assertThat(coldStats.getTotalShardCount(), is(1));
        assertThat(coldStats.getDocCount(), is(coldStats.getTotalShardCount() * docCount));
        assertThat(coldStats.getTotalSize(), is(coldStats.getTotalShardCount() * byteSize));
    }
}
