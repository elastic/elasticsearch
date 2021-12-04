/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Collections;

public class BalancedShardsAllocatorTests extends ESAllocationTestCase {
    public void testDecideShardAllocation() {
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", false, ShardRoutingState.STARTED);
        assertEquals(clusterState.nodes().getSize(), 3);
        clusterState = allocateNew(clusterState);

        ShardRouting shard = clusterState.routingTable().index("idx_new").shard(0).primaryShard();
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            new RoutingNodes(clusterState, false),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
        allocation.debugDecision(true);

        BalancedShardsAllocator.WeightFunction weightFunction = new BalancedShardsAllocator.WeightFunction(0.45f, 0.55f);
        BalancedShardsAllocator.Balancer balancer = new BalancedShardsAllocator.Balancer(logger, allocation, weightFunction, 1.0f);
        float minWeight = Float.POSITIVE_INFINITY;
        String minWeightNode = null;
        for (BalancedShardsAllocator.ModelNode node : balancer.nodesArray()) {
            float weight = weightFunction.weight(balancer, node, shard.getIndexName());
            if (weight < minWeight) {
                minWeight = weight;
                minWeightNode = node.getNodeId();
            }
        }
        AllocateUnassignedDecision allocateDecision = allocator.decideShardAllocation(shard, allocation).getAllocateDecision();

        assertEquals(minWeightNode, allocateDecision.getTargetNode().getId());
    }

    private ClusterState allocateNew(ClusterState state) {
        String index = "idx_new";
        Metadata metadata = Metadata.builder(state.metadata())
            .put(IndexMetadata.builder(index).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(state.routingTable()).addAsNew(metadata.index(index)).build();

        ClusterState clusterState = ClusterState.builder(state).metadata(metadata).routingTable(initialRoutingTable).build();
        return clusterState;
    }
}
