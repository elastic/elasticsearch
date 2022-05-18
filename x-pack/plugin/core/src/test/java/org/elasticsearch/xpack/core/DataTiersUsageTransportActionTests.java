/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataTiersUsageTransportActionTests extends ESTestCase {

    public void testCalculateMAD() {
        assertThat(DataTiersUsageTransportAction.computeMedianAbsoluteDeviation(new TDigestState(10)), equalTo(0L));

        TDigestState sketch = new TDigestState(randomDoubleBetween(1, 1000, false));
        sketch.add(1);
        sketch.add(1);
        sketch.add(2);
        sketch.add(2);
        sketch.add(4);
        sketch.add(6);
        sketch.add(9);
        assertThat(DataTiersUsageTransportAction.computeMedianAbsoluteDeviation(sketch), equalTo(1L));
    }

    public void testTierIndices() {
        IndexMetadata hotIndex1 = indexMetadata("hot-1", 1, 0, DataTier.DATA_HOT);
        IndexMetadata hotIndex2 = indexMetadata("hot-2", 1, 0, DataTier.DATA_HOT);
        IndexMetadata warmIndex1 = indexMetadata("warm-1", 1, 0, DataTier.DATA_WARM);
        IndexMetadata coldIndex1 = indexMetadata("cold-1", 1, 0, DataTier.DATA_COLD);
        IndexMetadata coldIndex2 = indexMetadata("cold-2", 1, 0, DataTier.DATA_COLD, DataTier.DATA_WARM); // Prefers cold over warm
        IndexMetadata nonTiered = indexMetadata("non-tier", 1, 0); // No tier

        ImmutableOpenMap.Builder<String, IndexMetadata> indicesBuilder = ImmutableOpenMap.builder();
        indicesBuilder.put("hot-1", hotIndex1);
        indicesBuilder.put("hot-2", hotIndex2);
        indicesBuilder.put("warm-1", warmIndex1);
        indicesBuilder.put("cold-1", coldIndex1);
        indicesBuilder.put("cold-2", coldIndex2);
        indicesBuilder.put("non-tier", nonTiered);
        ImmutableOpenMap<String, IndexMetadata> indices = indicesBuilder.build();

        Map<String, String> tiers = DataTiersUsageTransportAction.tierIndices(indices);
        assertThat(tiers.size(), equalTo(5));
        assertThat(tiers.get("hot-1"), equalTo(DataTier.DATA_HOT));
        assertThat(tiers.get("hot-2"), equalTo(DataTier.DATA_HOT));
        assertThat(tiers.get("warm-1"), equalTo(DataTier.DATA_WARM));
        assertThat(tiers.get("cold-1"), equalTo(DataTier.DATA_COLD));
        assertThat(tiers.get("cold-2"), equalTo(DataTier.DATA_COLD));
        assertThat(tiers.get("non-tier"), nullValue());
    }

    public void testCalculateStatsNoTiers() {
        // Nodes: 0 Tiered Nodes, 1 Data Node
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        DiscoveryNode leader = newNode(0, DiscoveryNodeRole.MASTER_ROLE);
        discoBuilder.add(leader);
        discoBuilder.masterNodeId(leader.getId());

        DiscoveryNode dataNode1 = newNode(1, DiscoveryNodeRole.DATA_ROLE);
        discoBuilder.add(dataNode1);

        discoBuilder.localNodeId(dataNode1.getId());

        // Indices: 1 Regular index
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        IndexMetadata index1 = indexMetadata("index_1", 3, 1);
        metadataBuilder.put(index1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index1.getIndex());
            routeTestShardToNodes(index1, 0, indexRoutingTableBuilder, dataNode1);
            routeTestShardToNodes(index1, 1, indexRoutingTableBuilder, dataNode1);
            routeTestShardToNodes(index1, 2, indexRoutingTableBuilder, dataNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        // Cluster State and create stats responses
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();

        long byteSize = randomLongBetween(1024L, 1024L * 1024L * 1024L * 30L); // 1 KB to 30 GB
        long docCount = randomLongBetween(100L, 100000000L); // one hundred to one hundred million
        List<NodeStats> nodeStatsList = buildNodeStats(clusterState, byteSize, docCount);

        // Calculate usage
        Map<String, String> indexByTier = DataTiersUsageTransportAction.tierIndices(clusterState.metadata().indices());
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeStatsList,
            indexByTier,
            clusterState.getRoutingNodes()
        );

        // Verify - No results when no tiers present
        assertThat(tierSpecificStats.size(), is(0));
    }

    public void testCalculateStatsTieredNodesOnly() {
        // Nodes: 1 Data, 1 Hot, 1 Warm, 1 Cold, 1 Frozen
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        DiscoveryNode leader = newNode(0, DiscoveryNodeRole.MASTER_ROLE);
        discoBuilder.add(leader);
        discoBuilder.masterNodeId(leader.getId());

        DiscoveryNode dataNode1 = newNode(1, DiscoveryNodeRole.DATA_ROLE);
        discoBuilder.add(dataNode1);
        DiscoveryNode hotNode1 = newNode(2, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        discoBuilder.add(hotNode1);
        DiscoveryNode warmNode1 = newNode(3, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        discoBuilder.add(warmNode1);
        DiscoveryNode coldNode1 = newNode(4, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        discoBuilder.add(coldNode1);
        DiscoveryNode frozenNode1 = newNode(5, DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
        discoBuilder.add(frozenNode1);

        discoBuilder.localNodeId(dataNode1.getId());

        // Indices: 1 Regular index, not hosted on any tiers
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        IndexMetadata index1 = indexMetadata("index_1", 3, 1);
        metadataBuilder.put(index1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index1.getIndex());
            routeTestShardToNodes(index1, 0, indexRoutingTableBuilder, dataNode1);
            routeTestShardToNodes(index1, 1, indexRoutingTableBuilder, dataNode1);
            routeTestShardToNodes(index1, 2, indexRoutingTableBuilder, dataNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        // Cluster State and create stats responses
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();

        long byteSize = randomLongBetween(1024L, 1024L * 1024L * 1024L * 30L); // 1 KB to 30 GB
        long docCount = randomLongBetween(100L, 100000000L); // one hundred to one hundred million
        List<NodeStats> nodeStatsList = buildNodeStats(clusterState, byteSize, docCount);

        // Calculate usage
        Map<String, String> indexByTier = DataTiersUsageTransportAction.tierIndices(clusterState.metadata().indices());
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeStatsList,
            indexByTier,
            clusterState.getRoutingNodes()
        );

        // Verify - Results are present but they lack index numbers because none are tiered
        assertThat(tierSpecificStats.size(), is(4));

        DataTiersFeatureSetUsage.TierSpecificStats hotStats = tierSpecificStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.nodeCount, is(1));
        assertThat(hotStats.indexCount, is(0));
        assertThat(hotStats.totalShardCount, is(0));
        assertThat(hotStats.docCount, is(0L));
        assertThat(hotStats.totalByteCount, is(0L));
        assertThat(hotStats.primaryShardCount, is(0));
        assertThat(hotStats.primaryByteCount, is(0L));
        assertThat(hotStats.primaryByteCountMedian, is(0L)); // All same size
        assertThat(hotStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats warmStats = tierSpecificStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.nodeCount, is(1));
        assertThat(warmStats.indexCount, is(0));
        assertThat(warmStats.totalShardCount, is(0));
        assertThat(warmStats.docCount, is(0L));
        assertThat(warmStats.totalByteCount, is(0L));
        assertThat(warmStats.primaryShardCount, is(0));
        assertThat(warmStats.primaryByteCount, is(0L));
        assertThat(warmStats.primaryByteCountMedian, is(0L)); // All same size
        assertThat(warmStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats coldStats = tierSpecificStats.get(DataTier.DATA_COLD);
        assertThat(coldStats, is(notNullValue()));
        assertThat(coldStats.nodeCount, is(1));
        assertThat(coldStats.indexCount, is(0));
        assertThat(coldStats.totalShardCount, is(0));
        assertThat(coldStats.docCount, is(0L));
        assertThat(coldStats.totalByteCount, is(0L));
        assertThat(coldStats.primaryShardCount, is(0));
        assertThat(coldStats.primaryByteCount, is(0L));
        assertThat(coldStats.primaryByteCountMedian, is(0L)); // All same size
        assertThat(coldStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats frozenStats = tierSpecificStats.get(DataTier.DATA_FROZEN);
        assertThat(frozenStats, is(notNullValue()));
        assertThat(frozenStats.nodeCount, is(1));
        assertThat(frozenStats.indexCount, is(0));
        assertThat(frozenStats.totalShardCount, is(0));
        assertThat(frozenStats.docCount, is(0L));
        assertThat(frozenStats.totalByteCount, is(0L));
        assertThat(frozenStats.primaryShardCount, is(0));
        assertThat(frozenStats.primaryByteCount, is(0L));
        assertThat(frozenStats.primaryByteCountMedian, is(0L)); // All same size
        assertThat(frozenStats.primaryShardBytesMAD, is(0L)); // All same size
    }

    public void testCalculateStatsTieredIndicesOnly() {
        // Nodes: 3 Data, 0 Tiered - Only hosting indices on generic data nodes
        int nodeId = 0;
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        DiscoveryNode leader = newNode(nodeId++, DiscoveryNodeRole.MASTER_ROLE);
        discoBuilder.add(leader);
        discoBuilder.masterNodeId(leader.getId());

        DiscoveryNode dataNode1 = newNode(nodeId++, DiscoveryNodeRole.DATA_ROLE);
        discoBuilder.add(dataNode1);
        DiscoveryNode dataNode2 = newNode(nodeId++, DiscoveryNodeRole.DATA_ROLE);
        discoBuilder.add(dataNode2);
        DiscoveryNode dataNode3 = newNode(nodeId, DiscoveryNodeRole.DATA_ROLE);
        discoBuilder.add(dataNode3);

        discoBuilder.localNodeId(dataNode1.getId());

        // Indices: 1 Hot index, 2 Warm indices, 3 Cold indices
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        IndexMetadata hotIndex1 = indexMetadata("hot_index_1", 3, 1, DataTier.DATA_HOT);
        metadataBuilder.put(hotIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(hotIndex1.getIndex());
            routeTestShardToNodes(hotIndex1, 0, indexRoutingTableBuilder, dataNode1, dataNode2);
            routeTestShardToNodes(hotIndex1, 1, indexRoutingTableBuilder, dataNode2, dataNode3);
            routeTestShardToNodes(hotIndex1, 2, indexRoutingTableBuilder, dataNode3, dataNode1);
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
            routeTestShardToNodes(warmIndex2, 0, indexRoutingTableBuilder, dataNode3, dataNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        IndexMetadata coldIndex1 = indexMetadata("cold_index_1", 1, 0, DataTier.DATA_COLD);
        metadataBuilder.put(coldIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(coldIndex1.getIndex());
            routeTestShardToNodes(coldIndex1, 0, indexRoutingTableBuilder, dataNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        IndexMetadata coldIndex2 = indexMetadata("cold_index_2", 1, 0, DataTier.DATA_COLD);
        metadataBuilder.put(coldIndex2, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(coldIndex2.getIndex());
            routeTestShardToNodes(coldIndex2, 0, indexRoutingTableBuilder, dataNode2);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        IndexMetadata coldIndex3 = indexMetadata("cold_index_3", 1, 0, DataTier.DATA_COLD);
        metadataBuilder.put(coldIndex3, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(coldIndex3.getIndex());
            routeTestShardToNodes(coldIndex3, 0, indexRoutingTableBuilder, dataNode3);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        // Cluster State and create stats responses
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();

        long byteSize = randomLongBetween(1024L, 1024L * 1024L * 1024L * 30L); // 1 KB to 30 GB
        long docCount = randomLongBetween(100L, 100000000L); // one hundred to one hundred million
        List<NodeStats> nodeStatsList = buildNodeStats(clusterState, byteSize, docCount);

        // Calculate usage
        Map<String, String> indexByTier = DataTiersUsageTransportAction.tierIndices(clusterState.metadata().indices());
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeStatsList,
            indexByTier,
            clusterState.getRoutingNodes()
        );

        // Verify - Index stats exist for the tiers, but no tiered nodes are found
        assertThat(tierSpecificStats.size(), is(3));

        DataTiersFeatureSetUsage.TierSpecificStats hotStats = tierSpecificStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.nodeCount, is(0));
        assertThat(hotStats.indexCount, is(1));
        assertThat(hotStats.totalShardCount, is(6));
        assertThat(hotStats.docCount, is(6 * docCount));
        assertThat(hotStats.totalByteCount, is(6 * byteSize));
        assertThat(hotStats.primaryShardCount, is(3));
        assertThat(hotStats.primaryByteCount, is(3 * byteSize));
        assertThat(hotStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(hotStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats warmStats = tierSpecificStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.nodeCount, is(0));
        assertThat(warmStats.indexCount, is(2));
        assertThat(warmStats.totalShardCount, is(4));
        assertThat(warmStats.docCount, is(4 * docCount));
        assertThat(warmStats.totalByteCount, is(4 * byteSize));
        assertThat(warmStats.primaryShardCount, is(2));
        assertThat(warmStats.primaryByteCount, is(2 * byteSize));
        assertThat(warmStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(warmStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats coldStats = tierSpecificStats.get(DataTier.DATA_COLD);
        assertThat(coldStats, is(notNullValue()));
        assertThat(coldStats.nodeCount, is(0));
        assertThat(coldStats.indexCount, is(3));
        assertThat(coldStats.totalShardCount, is(3));
        assertThat(coldStats.docCount, is(3 * docCount));
        assertThat(coldStats.totalByteCount, is(3 * byteSize));
        assertThat(coldStats.primaryShardCount, is(3));
        assertThat(coldStats.primaryByteCount, is(3 * byteSize));
        assertThat(coldStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(coldStats.primaryShardBytesMAD, is(0L)); // All same size
    }

    public void testCalculateStatsReasonableCase() {
        // Nodes: 3 Hot, 5 Warm, 1 Cold
        int nodeId = 0;
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        DiscoveryNode leader = newNode(nodeId++, DiscoveryNodeRole.MASTER_ROLE);
        discoBuilder.add(leader);
        discoBuilder.masterNodeId(leader.getId());

        DiscoveryNode hotNode1 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        discoBuilder.add(hotNode1);
        DiscoveryNode hotNode2 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        discoBuilder.add(hotNode2);
        DiscoveryNode hotNode3 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        discoBuilder.add(hotNode3);
        DiscoveryNode warmNode1 = newNode(nodeId++, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        discoBuilder.add(warmNode1);
        DiscoveryNode warmNode2 = newNode(nodeId++, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        discoBuilder.add(warmNode2);
        DiscoveryNode warmNode3 = newNode(nodeId++, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        discoBuilder.add(warmNode3);
        DiscoveryNode warmNode4 = newNode(nodeId++, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        discoBuilder.add(warmNode4);
        DiscoveryNode warmNode5 = newNode(nodeId++, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        discoBuilder.add(warmNode5);
        DiscoveryNode coldNode1 = newNode(nodeId, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        discoBuilder.add(coldNode1);

        discoBuilder.localNodeId(hotNode1.getId());

        // Indices: 1 Hot index, 2 Warm indices, 3 Cold indices
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        IndexMetadata hotIndex1 = indexMetadata("hot_index_1", 3, 1, DataTier.DATA_HOT);
        metadataBuilder.put(hotIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(hotIndex1.getIndex());
            routeTestShardToNodes(hotIndex1, 0, indexRoutingTableBuilder, hotNode1, hotNode2);
            routeTestShardToNodes(hotIndex1, 1, indexRoutingTableBuilder, hotNode2, hotNode3);
            routeTestShardToNodes(hotIndex1, 2, indexRoutingTableBuilder, hotNode3, hotNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        IndexMetadata warmIndex1 = indexMetadata("warm_index_1", 1, 1, DataTier.DATA_WARM);
        metadataBuilder.put(warmIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(warmIndex1.getIndex());
            routeTestShardToNodes(warmIndex1, 0, indexRoutingTableBuilder, warmNode1, warmNode2);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        IndexMetadata warmIndex2 = indexMetadata("warm_index_2", 1, 1, DataTier.DATA_WARM);
        metadataBuilder.put(warmIndex2, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(warmIndex2.getIndex());
            routeTestShardToNodes(warmIndex2, 0, indexRoutingTableBuilder, warmNode3, warmNode4);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        IndexMetadata coldIndex1 = indexMetadata("cold_index_1", 1, 0, DataTier.DATA_COLD);
        metadataBuilder.put(coldIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(coldIndex1.getIndex());
            routeTestShardToNodes(coldIndex1, 0, indexRoutingTableBuilder, coldNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        IndexMetadata coldIndex2 = indexMetadata("cold_index_2", 1, 0, DataTier.DATA_COLD);
        metadataBuilder.put(coldIndex2, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(coldIndex2.getIndex());
            routeTestShardToNodes(coldIndex2, 0, indexRoutingTableBuilder, coldNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        IndexMetadata coldIndex3 = indexMetadata("cold_index_3", 1, 0, DataTier.DATA_COLD);
        metadataBuilder.put(coldIndex3, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(coldIndex3.getIndex());
            routeTestShardToNodes(coldIndex3, 0, indexRoutingTableBuilder, coldNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        // Cluster State and create stats responses
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();

        long byteSize = randomLongBetween(1024L, 1024L * 1024L * 1024L * 30L); // 1 KB to 30 GB
        long docCount = randomLongBetween(100L, 100000000L); // one hundred to one hundred million
        List<NodeStats> nodeStatsList = buildNodeStats(clusterState, byteSize, docCount);

        // Calculate usage
        Map<String, String> indexByTier = DataTiersUsageTransportAction.tierIndices(clusterState.metadata().indices());
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeStatsList,
            indexByTier,
            clusterState.getRoutingNodes()
        );

        // Verify - Node and Index stats are both collected
        assertThat(tierSpecificStats.size(), is(3));

        DataTiersFeatureSetUsage.TierSpecificStats hotStats = tierSpecificStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.nodeCount, is(3));
        assertThat(hotStats.indexCount, is(1));
        assertThat(hotStats.totalShardCount, is(6));
        assertThat(hotStats.docCount, is(6 * docCount));
        assertThat(hotStats.totalByteCount, is(6 * byteSize));
        assertThat(hotStats.primaryShardCount, is(3));
        assertThat(hotStats.primaryByteCount, is(3 * byteSize));
        assertThat(hotStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(hotStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats warmStats = tierSpecificStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.nodeCount, is(5));
        assertThat(warmStats.indexCount, is(2));
        assertThat(warmStats.totalShardCount, is(4));
        assertThat(warmStats.docCount, is(4 * docCount));
        assertThat(warmStats.totalByteCount, is(4 * byteSize));
        assertThat(warmStats.primaryShardCount, is(2));
        assertThat(warmStats.primaryByteCount, is(2 * byteSize));
        assertThat(warmStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(warmStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats coldStats = tierSpecificStats.get(DataTier.DATA_COLD);
        assertThat(coldStats, is(notNullValue()));
        assertThat(coldStats.nodeCount, is(1));
        assertThat(coldStats.indexCount, is(3));
        assertThat(coldStats.totalShardCount, is(3));
        assertThat(coldStats.docCount, is(3 * docCount));
        assertThat(coldStats.totalByteCount, is(3 * byteSize));
        assertThat(coldStats.primaryShardCount, is(3));
        assertThat(coldStats.primaryByteCount, is(3 * byteSize));
        assertThat(coldStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(coldStats.primaryShardBytesMAD, is(0L)); // All same size
    }

    public void testCalculateStatsMixedTiers() {
        // Nodes: 3 Hot+Warm - Nodes that are marked as part of multiple tiers
        int nodeId = 0;
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        DiscoveryNode leader = newNode(nodeId++, DiscoveryNodeRole.MASTER_ROLE);
        discoBuilder.add(leader);
        discoBuilder.masterNodeId(leader.getId());

        DiscoveryNode mixedNode1 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        discoBuilder.add(mixedNode1);
        DiscoveryNode mixedNode2 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        discoBuilder.add(mixedNode2);
        DiscoveryNode mixedNode3 = newNode(nodeId, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        discoBuilder.add(mixedNode3);

        discoBuilder.localNodeId(mixedNode1.getId());

        // Indices: 1 Hot index, 2 Warm indices
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        IndexMetadata hotIndex1 = indexMetadata("hot_index_1", 3, 1, DataTier.DATA_HOT);
        metadataBuilder.put(hotIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(hotIndex1.getIndex());
            routeTestShardToNodes(hotIndex1, 0, indexRoutingTableBuilder, mixedNode1, mixedNode2);
            routeTestShardToNodes(hotIndex1, 1, indexRoutingTableBuilder, mixedNode3, mixedNode1);
            routeTestShardToNodes(hotIndex1, 2, indexRoutingTableBuilder, mixedNode2, mixedNode3);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        IndexMetadata warmIndex1 = indexMetadata("warm_index_1", 1, 1, DataTier.DATA_WARM);
        metadataBuilder.put(warmIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(warmIndex1.getIndex());
            routeTestShardToNodes(warmIndex1, 0, indexRoutingTableBuilder, mixedNode1, mixedNode2);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        IndexMetadata warmIndex2 = indexMetadata("warm_index_2", 1, 1, DataTier.DATA_WARM);
        metadataBuilder.put(warmIndex2, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(warmIndex2.getIndex());
            routeTestShardToNodes(warmIndex2, 0, indexRoutingTableBuilder, mixedNode3, mixedNode1);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        // Cluster State and create stats responses
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();

        long byteSize = randomLongBetween(1024L, 1024L * 1024L * 1024L * 30L); // 1 KB to 30 GB
        long docCount = randomLongBetween(100L, 100000000L); // one hundred to one hundred million
        List<NodeStats> nodeStatsList = buildNodeStats(clusterState, byteSize, docCount);

        // Calculate usage
        Map<String, String> indexByTier = DataTiersUsageTransportAction.tierIndices(clusterState.metadata().indices());
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeStatsList,
            indexByTier,
            clusterState.getRoutingNodes()
        );

        // Verify - Index stats are separated by their preferred tier, instead of counted
        // toward multiple tiers based on their current routing. Nodes are counted for each tier they are in.
        assertThat(tierSpecificStats.size(), is(2));

        DataTiersFeatureSetUsage.TierSpecificStats hotStats = tierSpecificStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.nodeCount, is(3));
        assertThat(hotStats.indexCount, is(1));
        assertThat(hotStats.totalShardCount, is(6));
        assertThat(hotStats.docCount, is(6 * docCount));
        assertThat(hotStats.totalByteCount, is(6 * byteSize));
        assertThat(hotStats.primaryShardCount, is(3));
        assertThat(hotStats.primaryByteCount, is(3 * byteSize));
        assertThat(hotStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(hotStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats warmStats = tierSpecificStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.nodeCount, is(3));
        assertThat(warmStats.indexCount, is(2));
        assertThat(warmStats.totalShardCount, is(4));
        assertThat(warmStats.docCount, is(4 * docCount));
        assertThat(warmStats.totalByteCount, is(4 * byteSize));
        assertThat(warmStats.primaryShardCount, is(2));
        assertThat(warmStats.primaryByteCount, is(2 * byteSize));
        assertThat(warmStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(warmStats.primaryShardBytesMAD, is(0L)); // All same size
    }

    public void testCalculateStatsStuckInWrongTier() {
        // Nodes: 3 Hot, 0 Warm - Emulating indices stuck on non-preferred tiers
        int nodeId = 0;
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        DiscoveryNode leader = newNode(nodeId++, DiscoveryNodeRole.MASTER_ROLE);
        discoBuilder.add(leader);
        discoBuilder.masterNodeId(leader.getId());

        DiscoveryNode hotNode1 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        discoBuilder.add(hotNode1);
        DiscoveryNode hotNode2 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        discoBuilder.add(hotNode2);
        DiscoveryNode hotNode3 = newNode(nodeId, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        discoBuilder.add(hotNode3);

        discoBuilder.localNodeId(hotNode1.getId());

        // Indices: 1 Hot index, 1 Warm index (Warm index is allocated to less preferred hot node because warm nodes are missing)
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        IndexMetadata hotIndex1 = indexMetadata("hot_index_1", 3, 1, DataTier.DATA_HOT);
        metadataBuilder.put(hotIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(hotIndex1.getIndex());
            routeTestShardToNodes(hotIndex1, 0, indexRoutingTableBuilder, hotNode1, hotNode2);
            routeTestShardToNodes(hotIndex1, 1, indexRoutingTableBuilder, hotNode3, hotNode1);
            routeTestShardToNodes(hotIndex1, 2, indexRoutingTableBuilder, hotNode2, hotNode3);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        IndexMetadata warmIndex1 = indexMetadata("warm_index_1", 1, 1, DataTier.DATA_WARM, DataTier.DATA_HOT);
        metadataBuilder.put(warmIndex1, false).generateClusterUuidIfNeeded();
        {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(warmIndex1.getIndex());
            routeTestShardToNodes(warmIndex1, 0, indexRoutingTableBuilder, hotNode1, hotNode2);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        // Cluster State and create stats responses
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();

        long byteSize = randomLongBetween(1024L, 1024L * 1024L * 1024L * 30L); // 1 KB to 30 GB
        long docCount = randomLongBetween(100L, 100000000L); // one hundred to one hundred million
        List<NodeStats> nodeStatsList = buildNodeStats(clusterState, byteSize, docCount);

        // Calculate usage
        Map<String, String> indexByTier = DataTiersUsageTransportAction.tierIndices(clusterState.metadata().indices());
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeStatsList,
            indexByTier,
            clusterState.getRoutingNodes()
        );

        // Verify - Warm indices are still calculated separately from Hot ones, despite Warm nodes missing
        assertThat(tierSpecificStats.size(), is(2));

        DataTiersFeatureSetUsage.TierSpecificStats hotStats = tierSpecificStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.nodeCount, is(3));
        assertThat(hotStats.indexCount, is(1));
        assertThat(hotStats.totalShardCount, is(6));
        assertThat(hotStats.docCount, is(6 * docCount));
        assertThat(hotStats.totalByteCount, is(6 * byteSize));
        assertThat(hotStats.primaryShardCount, is(3));
        assertThat(hotStats.primaryByteCount, is(3 * byteSize));
        assertThat(hotStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(hotStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats warmStats = tierSpecificStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.nodeCount, is(0));
        assertThat(warmStats.indexCount, is(1));
        assertThat(warmStats.totalShardCount, is(2));
        assertThat(warmStats.docCount, is(2 * docCount));
        assertThat(warmStats.totalByteCount, is(2 * byteSize));
        assertThat(warmStats.primaryShardCount, is(1));
        assertThat(warmStats.primaryByteCount, is(byteSize));
        assertThat(warmStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(warmStats.primaryShardBytesMAD, is(0L)); // All same size
    }

    private static DiscoveryNode newNode(int nodeId, DiscoveryNodeRole... roles) {
        return new DiscoveryNode(
            "node_" + nodeId,
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(roles),
            Version.CURRENT
        );
    }

    private static IndexMetadata indexMetadata(String indexName, int numberOfShards, int numberOfReplicas, String... dataTierPrefs) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .put(SETTING_CREATION_DATE, System.currentTimeMillis());

        if (dataTierPrefs.length > 1) {
            StringBuilder tierBuilder = new StringBuilder(dataTierPrefs[0]);
            for (int idx = 1; idx < dataTierPrefs.length; idx++) {
                tierBuilder.append(',').append(dataTierPrefs[idx]);
            }
            settingsBuilder.put(DataTier.TIER_PREFERENCE, tierBuilder.toString());
        } else if (dataTierPrefs.length == 1) {
            settingsBuilder.put(DataTier.TIER_PREFERENCE, dataTierPrefs[0]);
        }

        return IndexMetadata.builder(indexName).settings(settingsBuilder.build()).timestampRange(IndexLongFieldRange.UNKNOWN).build();
    }

    private static void routeTestShardToNodes(
        IndexMetadata index,
        int shard,
        IndexRoutingTable.Builder indexRoutingTableBuilder,
        DiscoveryNode... nodes
    ) {
        ShardId shardId = new ShardId(index.getIndex(), shard);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
        boolean primary = true;
        for (DiscoveryNode node : nodes) {
            indexShardRoutingBuilder.addShard(
                TestShardRouting.newShardRouting(shardId, node.getId(), null, primary, ShardRoutingState.STARTED)
            );
            primary = false;
        }
        indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
    }

    private List<NodeStats> buildNodeStats(ClusterState clusterState, long bytesPerShard, long docsPerShard) {
        DiscoveryNodes nodes = clusterState.getNodes();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        List<NodeStats> nodeStatsList = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            RoutingNode routingNode = routingNodes.node(node.getId());
            if (routingNode == null) {
                continue;
            }
            Map<Index, List<IndexShardStats>> indexStats = new HashMap<>();
            for (ShardRouting shardRouting : routingNode) {
                ShardId shardId = shardRouting.shardId();
                ShardStats shardStat = shardStat(bytesPerShard, docsPerShard, shardRouting);
                IndexShardStats shardStats = new IndexShardStats(shardId, new ShardStats[] { shardStat });
                indexStats.computeIfAbsent(shardId.getIndex(), k -> new ArrayList<>()).add(shardStats);
            }
            NodeIndicesStats nodeIndexStats = new NodeIndicesStats(new CommonStats(), indexStats);
            nodeStatsList.add(mockNodeStats(node, nodeIndexStats));
        }
        return nodeStatsList;
    }

    private static ShardStats shardStat(long byteCount, long docCount, ShardRouting routing) {
        StoreStats storeStats = new StoreStats(randomNonNegativeLong(), byteCount, 0L);
        DocsStats docsStats = new DocsStats(docCount, 0L, byteCount);

        CommonStats commonStats = new CommonStats(CommonStatsFlags.ALL);
        commonStats.getStore().add(storeStats);
        commonStats.getDocs().add(docsStats);

        Path fakePath = PathUtils.get("test/dir/" + routing.shardId().getIndex().getUUID() + "/" + routing.shardId().id());
        ShardPath fakeShardPath = new ShardPath(false, fakePath, fakePath, routing.shardId());

        return new ShardStats(routing, fakeShardPath, commonStats, null, null, null);
    }

    private static NodeStats mockNodeStats(DiscoveryNode node, NodeIndicesStats indexStats) {
        NodeStats stats = mock(NodeStats.class);
        when(stats.getNode()).thenReturn(node);
        when(stats.getIndices()).thenReturn(indexStats);
        return stats;
    }
}
