/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class IndexAllocationTests extends ESTestCase {
    private final Index content = idx("content");
    private final Index hot = idx("hot");
    private final Index warm = idx("warm");
    private final Index cold = idx("cold");
    private final Index frozen = idx("frozen");

    public void testEmptyIndicesNotOnWarmColdTier() {
        assertFalse(IndexAllocation.isAnyOnWarmOrColdTier(clusterState(), Collections.emptyList()));
    }

    public void testOtherIndicesNotOnWarmColdTier() {
        assertFalse(IndexAllocation.isAnyOnWarmOrColdTier(clusterState(), List.of(hot, frozen)));
    }

    public void testIndicesOnContentNodeNotOnWarmColdTier() {
        assertFalse(IndexAllocation.isAnyOnWarmOrColdTier(clusterState(), List.of(content)));
    }

    public void testIndicesOnWarmColdTier() {
        assertTrue(IndexAllocation.isAnyOnWarmOrColdTier(clusterState(), List.of(warm)));
        assertTrue(IndexAllocation.isAnyOnWarmOrColdTier(clusterState(), List.of(cold)));
    }

    public void testMixedIndicesOnWarmColdTier() {
        assertTrue(IndexAllocation.isAnyOnWarmOrColdTier(clusterState(), List.of(hot, warm)));
        assertTrue(IndexAllocation.isAnyOnWarmOrColdTier(clusterState(), List.of(frozen, cold)));
    }

    /**
     * Creates a cluster state that represents several indices:
     *
     * <ul>
     *     <li><code>hot</code> assigned to a hot-tier node named <code>n-hot</code></li>
     *     <li><code>warm</code> assigned to a warm-tier node named <code>n-warm</code></li>
     *     <li><code>cold</code> assigned to a cold-tier node named <code>n-cold</code></li>
     *     <li><code>frozen</code> assigned to a frozen-tier node named <code>n-frozen</code></li>
     * </ul>
     */
    private ClusterState clusterState() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node);

        nodesBuilder.add(
            DiscoveryNodeUtils.builder("n-" + content.getName())
                .roles(
                    Set.of(
                        // content nodes have all roles
                        DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE,
                        DiscoveryNodeRole.DATA_HOT_NODE_ROLE,
                        DiscoveryNodeRole.DATA_WARM_NODE_ROLE,
                        DiscoveryNodeRole.DATA_COLD_NODE_ROLE,
                        DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE
                    )
                )
                .build()
        );
        nodesBuilder.add(DiscoveryNodeUtils.builder("n-" + hot.getName()).roles(Set.of(DiscoveryNodeRole.DATA_HOT_NODE_ROLE)).build());
        nodesBuilder.add(DiscoveryNodeUtils.builder("n-" + warm.getName()).roles(Set.of(DiscoveryNodeRole.DATA_WARM_NODE_ROLE)).build());
        nodesBuilder.add(DiscoveryNodeUtils.builder("n-" + cold.getName()).roles(Set.of(DiscoveryNodeRole.DATA_COLD_NODE_ROLE)).build());
        nodesBuilder.add(
            DiscoveryNodeUtils.builder("n-" + frozen.getName()).roles(Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)).build()
        );

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Map<String, IndexMetadata> indices = new HashMap<>();
        for (Index index : List.of(content, hot, warm, cold, frozen)) {
            indices.put(index.getName(), metadata(index));
            ShardRouting shardRouting = ShardRouting.newUnassigned(
                new ShardId(index, 0),
                true,
                RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
                ShardRouting.Role.DEFAULT
            );

            shardRouting = shardRouting.initialize("n-" + index.getName(), null, 0).moveToStarted(0);
            routingTableBuilder.add(
                IndexRoutingTable.builder(index)
                    .addIndexShard(IndexShardRoutingTable.builder(shardRouting.shardId()).addShard(shardRouting))
            );
        }

        return ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().indices(indices).build())
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodesBuilder)
            .routingTable(routingTableBuilder)
            .build();
    }

    private IndexMetadata metadata(Index index) {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        return IndexMetadata.builder(index.getName()).settings(settings).numberOfShards(1).numberOfReplicas(0).build();
    }

    private Index idx(String name) {
        return new Index(name, UUID.randomUUID().toString());
    }

}
