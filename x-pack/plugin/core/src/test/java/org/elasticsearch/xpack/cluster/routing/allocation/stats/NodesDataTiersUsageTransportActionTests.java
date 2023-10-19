/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation.stats;

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
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NodesDataTiersUsageTransportActionTests extends ESTestCase {

    private static final CommonStats COMMON_STATS = new CommonStats(
        CommonStatsFlags.NONE.set(CommonStatsFlags.Flag.Docs, true).set(CommonStatsFlags.Flag.Store, true)
    );
    private long byteSize;
    private long docCount;

    @Before
    public void setup() {
        byteSize = randomLongBetween(1024L, 1024L * 1024L * 1024L * 30L); // 1 KB to 30 GB
        docCount = randomLongBetween(100L, 100000000L); // one hundred to one hundred million
    }

    public void testTierIndices() {
        IndexMetadata hotIndex1 = indexMetadata("hot-1", 1, 0, DataTier.DATA_HOT);
        IndexMetadata hotIndex2 = indexMetadata("hot-2", 1, 0, DataTier.DATA_HOT);
        IndexMetadata warmIndex1 = indexMetadata("warm-1", 1, 0, DataTier.DATA_WARM);
        IndexMetadata coldIndex1 = indexMetadata("cold-1", 1, 0, DataTier.DATA_COLD);
        IndexMetadata coldIndex2 = indexMetadata("cold-2", 1, 0, DataTier.DATA_COLD, DataTier.DATA_WARM); // Prefers cold over warm
        IndexMetadata nonTiered = indexMetadata("non-tier", 1, 0); // No tier
        IndexMetadata invalidTiered = indexMetadata("non-tier", 1, 0, "invalid-tier"); // No tier

        assertThat(NodesDataTiersUsageTransportAction.findPreferredTier(hotIndex1), equalTo(DataTier.DATA_HOT));
        assertThat(NodesDataTiersUsageTransportAction.findPreferredTier(hotIndex2), equalTo(DataTier.DATA_HOT));
        assertThat(NodesDataTiersUsageTransportAction.findPreferredTier(warmIndex1), equalTo(DataTier.DATA_WARM));
        assertThat(NodesDataTiersUsageTransportAction.findPreferredTier(coldIndex1), equalTo(DataTier.DATA_COLD));
        assertThat(NodesDataTiersUsageTransportAction.findPreferredTier(coldIndex2), equalTo(DataTier.DATA_COLD));
        assertThat(NodesDataTiersUsageTransportAction.findPreferredTier(nonTiered), nullValue());
        assertThat(NodesDataTiersUsageTransportAction.findPreferredTier(invalidTiered), nullValue());
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
        Map<String, NodeDataTiersUsage.UsageStats> usageStats = NodesDataTiersUsageTransportAction.calculateUsage(
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
        Map<String, NodeDataTiersUsage.UsageStats> usageStats = NodesDataTiersUsageTransportAction.calculateUsage(
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
        DiscoveryNode dataNode2 = newNode(nodeId++, DiscoveryNodeRole.DATA_ROLE);
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
        Map<String, NodeDataTiersUsage.UsageStats> usageStats = NodesDataTiersUsageTransportAction.calculateUsage(
            clusterState.getRoutingNodes().node(dataNode1.getId()),
            clusterState.metadata(),
            nodeIndicesStats
        );

        // Verify - Index stats exist for the tiers, but no tiered nodes are found
        assertThat(usageStats.size(), is(3));

        NodeDataTiersUsage.UsageStats hotStats = usageStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.getIndices(), equalTo(Set.of(hotIndex1.getIndex().getName())));
        assertThat(hotStats.getPrimaryShardSizes(), equalTo(List.of(byteSize)));
        assertThat(hotStats.getTotalShardCount(), is(2));
        assertThat(hotStats.getDocCount(), is(hotStats.getTotalShardCount() * docCount));
        assertThat(hotStats.getTotalSize(), is(hotStats.getTotalShardCount() * byteSize));

        NodeDataTiersUsage.UsageStats warmStats = usageStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.getIndices(), equalTo(Set.of(warmIndex1.getIndex().getName(), warmIndex2.getIndex().getName())));
        assertThat(warmStats.getPrimaryShardSizes(), equalTo(List.of(byteSize)));
        assertThat(warmStats.getTotalShardCount(), is(2));
        assertThat(warmStats.getDocCount(), is(warmStats.getTotalShardCount() * docCount));
        assertThat(warmStats.getTotalSize(), is(warmStats.getTotalShardCount() * byteSize));

        NodeDataTiersUsage.UsageStats coldStats = usageStats.get(DataTier.DATA_COLD);
        assertThat(coldStats, is(notNullValue()));
        assertThat(coldStats.getIndices(), equalTo(Set.of(coldIndex1.getIndex().getName())));
        assertThat(coldStats.getPrimaryShardSizes(), equalTo(List.of(byteSize)));
        assertThat(coldStats.getTotalShardCount(), is(1));
        assertThat(coldStats.getDocCount(), is(coldStats.getTotalShardCount() * docCount));
        assertThat(coldStats.getTotalSize(), is(coldStats.getTotalShardCount() * byteSize));
    }

    private static DiscoveryNode newNode(int nodeId, DiscoveryNodeRole... roles) {
        return DiscoveryNodeUtils.builder("node_" + nodeId).roles(Set.of(roles)).build();
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
        indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder);
    }

    private NodeIndicesStats buildNodeIndicesStats(RoutingNode routingNode, long bytesPerShard, long docsPerShard) {
        Map<Index, List<IndexShardStats>> indexStats = new HashMap<>();
        for (ShardRouting shardRouting : routingNode) {
            ShardId shardId = shardRouting.shardId();
            ShardStats shardStat = shardStat(bytesPerShard, docsPerShard, shardRouting);
            IndexShardStats shardStats = new IndexShardStats(shardId, new ShardStats[] { shardStat });
            indexStats.computeIfAbsent(shardId.getIndex(), k -> new ArrayList<>()).add(shardStats);
        }
        return new NodeIndicesStats(COMMON_STATS, Map.of(), indexStats, true);
    }

    private static ShardStats shardStat(long byteCount, long docCount, ShardRouting routing) {
        StoreStats storeStats = new StoreStats(randomNonNegativeLong(), byteCount, 0L);
        DocsStats docsStats = new DocsStats(docCount, 0L, byteCount);
        Path fakePath = PathUtils.get("test/dir/" + routing.shardId().getIndex().getUUID() + "/" + routing.shardId().id());
        ShardPath fakeShardPath = new ShardPath(false, fakePath, fakePath, routing.shardId());
        CommonStats commonStats = new CommonStats(CommonStatsFlags.ALL);
        commonStats.getStore().add(storeStats);
        commonStats.getDocs().add(docsStats);
        return new ShardStats(routing, fakeShardPath, commonStats, null, null, null, false, 0);
    }

    private static IndexMetadata indexMetadata(String indexName, int numberOfShards, int numberOfReplicas, String... dataTierPrefs) {
        Settings.Builder settingsBuilder = indexSettings(IndexVersion.current(), numberOfShards, numberOfReplicas).put(
            SETTING_CREATION_DATE,
            System.currentTimeMillis()
        );

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

}
