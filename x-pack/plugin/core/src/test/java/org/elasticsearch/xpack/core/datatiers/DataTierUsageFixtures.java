/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datatiers;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;

class DataTierUsageFixtures extends ESTestCase {

    private static final CommonStats COMMON_STATS = new CommonStats(
        CommonStatsFlags.NONE.set(CommonStatsFlags.Flag.Docs, true).set(CommonStatsFlags.Flag.Store, true)
    );

    static DiscoveryNode newNode(int nodeId, DiscoveryNodeRole... roles) {
        return DiscoveryNodeUtils.builder("node_" + nodeId).roles(Set.of(roles)).build();
    }

    static void routeTestShardToNodes(
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

    static NodeIndicesStats buildNodeIndicesStats(RoutingNode routingNode, long bytesPerShard, long docsPerShard) {
        Map<Index, List<IndexShardStats>> indexStats = new HashMap<>();
        for (ShardRouting shardRouting : routingNode) {
            ShardId shardId = shardRouting.shardId();
            ShardStats shardStat = shardStat(bytesPerShard, docsPerShard, shardRouting);
            IndexShardStats shardStats = new IndexShardStats(shardId, new ShardStats[] { shardStat });
            indexStats.computeIfAbsent(shardId.getIndex(), k -> new ArrayList<>()).add(shardStats);
        }
        return new NodeIndicesStats(
            COMMON_STATS,
            Map.of(),
            indexStats,
            // projectsByIndex is not needed as it is only used for rendering as XContent:
            Map.of(),
            true
        );
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

    static IndexMetadata indexMetadata(String indexName, int numberOfShards, int numberOfReplicas, String... dataTierPrefs) {
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
