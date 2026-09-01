/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.closeTo;

public class MutableRoutingAllocationTests extends ESAllocationTestCase {

    /**
     * When a shard becomes STARTED on a node, its write load now contributes to the node's proportion. The cached value
     * is invalidated so the next call recomputes from the updated routing state.
     */
    public void testCacheInvalidatedOnShardStarted() {
        String sourceNodeId = randomIdentifier();
        String otherNodeId = randomValueOtherThan(sourceNodeId, ESTestCase::randomIdentifier);

        Index index = new Index("test-index", "_na_");
        ShardId startedShardId = new ShardId(index, 0);    // write load 3.0, always STARTED
        ShardId initializingShardId = new ShardId(index, 1); // write load 7.0, starts INITIALIZING

        // With only shard0 started: proportion = 3.0/3.0 = 1.0
        // After shard1 also starts: proportion = max(3.0, 7.0)/(3.0+7.0) = 0.7
        ClusterInfo clusterInfo = ClusterInfo.builder().shardWriteLoads(Map.of(startedShardId, 3.0, initializingShardId, 7.0)).build();

        ClusterState clusterState = clusterStateWithShards(
            sourceNodeId,
            otherNodeId,
            index,
            TestShardRouting.newShardRouting(startedShardId, sourceNodeId, true, ShardRoutingState.STARTED),
            TestShardRouting.newShardRouting(initializingShardId, sourceNodeId, true, ShardRoutingState.INITIALIZING)
        );

        MutableRoutingAllocation allocation = newAllocation(clusterState, clusterInfo);
        RoutingNode node = allocation.routingNodes().node(sourceNodeId);

        // prime cache with only shard0 started
        assertThat(allocation.maxShardWriteLoadProportionForNode(node), closeTo(1.0, 1e-9));

        // startShard updates the routing node AND fires the cache-invalidation observer
        ShardRouting initializingShard = node.getByShardId(initializingShardId);
        allocation.routingNodes().startShard(initializingShard, allocation.changes(), 0L);

        // cache was invalidated; recomputed with both shards started → 0.7
        assertThat(allocation.maxShardWriteLoadProportionForNode(node), closeTo(0.7, 1e-9));
    }

    /**
     * When a shard starts relocating away from a node, it leaves the STARTED set. The cached proportion is invalidated
     * so the next call reflects only the remaining started shards.
     */
    public void testCacheInvalidatedOnRelocationStarted() {
        String sourceNodeId = randomIdentifier();
        String targetNodeId = randomValueOtherThan(sourceNodeId, ESTestCase::randomIdentifier);

        Index index = new Index("test-index", "_na_");
        ShardId relocatingShardId = new ShardId(index, 0); // write load 3.0, will relocate away
        ShardId stayingShardId = new ShardId(index, 1);    // write load 7.0, stays started

        // With both shards started: proportion = max(3.0, 7.0)/(3.0+7.0) = 0.7
        // After shard0 starts reloc: proportion = 7.0/7.0 = 1.0
        ClusterInfo clusterInfo = ClusterInfo.builder().shardWriteLoads(Map.of(relocatingShardId, 3.0, stayingShardId, 7.0)).build();

        ClusterState clusterState = clusterStateWithShards(
            sourceNodeId,
            targetNodeId,
            index,
            TestShardRouting.newShardRouting(relocatingShardId, sourceNodeId, true, ShardRoutingState.STARTED),
            TestShardRouting.newShardRouting(stayingShardId, sourceNodeId, true, ShardRoutingState.STARTED)
        );

        MutableRoutingAllocation allocation = newAllocation(clusterState, clusterInfo);
        RoutingNode node = allocation.routingNodes().node(sourceNodeId);

        // prime cache with both shards started
        assertThat(allocation.maxShardWriteLoadProportionForNode(node), closeTo(0.7, 1e-9));

        // relocateShard moves shard0 to RELOCATING on sourceNode and fires the cache-invalidation observer
        ShardRouting relocating = node.getByShardId(relocatingShardId);
        allocation.routingNodes().relocateShard(relocating, targetNodeId, 0L, "test", allocation.changes());

        // cache was invalidated; recomputed with only shard1 started → 1.0
        assertThat(allocation.maxShardWriteLoadProportionForNode(node), closeTo(1.0, 1e-9));
    }

    private static MutableRoutingAllocation newAllocation(ClusterState clusterState, ClusterInfo clusterInfo) {
        RoutingAllocation mutable = TestRoutingAllocationFactory.forClusterState(clusterState).clusterInfo(clusterInfo).mutable();
        return (MutableRoutingAllocation) (randomBoolean() ? mutable.mutableCloneForSimulation() : mutable);
    }

    private static ClusterState clusterStateWithShards(String primaryNodeId, String otherNodeId, Index index, ShardRouting... shards) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), shards.length, 0))
            .build();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index);
        for (ShardRouting shard : shards) {
            indexRoutingTable.addShard(shard);
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode(primaryNodeId)).add(newNode(otherNodeId)))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(indexRoutingTable.build()).build())
            .build();
    }
}
