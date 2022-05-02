/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

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
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Collections;
import java.util.List;

public class BalancedShardsAllocatorTests extends ESAllocationTestCase {

    public void testDecideShardAllocation() {
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", false, ShardRoutingState.STARTED);
        assertEquals(clusterState.nodes().getSize(), 3);

        // add new index
        String index = "idx_new";
        Metadata metadata = Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder(index).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();
        RoutingTable initialRoutingTable = RoutingTable.builder(clusterState.routingTable()).addAsNew(metadata.index(index)).build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(initialRoutingTable).build();

        ShardRouting shard = clusterState.routingTable().index("idx_new").shard(0).primaryShard();
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            RoutingNodes.mutable(clusterState.routingTable(), clusterState.nodes()),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );

        allocation.debugDecision(false);
        AllocateUnassignedDecision allocateDecision = allocator.decideShardAllocation(shard, allocation).getAllocateDecision();
        allocation.debugDecision(true);
        AllocateUnassignedDecision allocateDecisionWithExplain = allocator.decideShardAllocation(shard, allocation).getAllocateDecision();
        // the allocation decision should have same target node no matter the debug is on or off
        assertEquals(allocateDecision.getTargetNode().getId(), allocateDecisionWithExplain.getTargetNode().getId());

        allocator.allocate(allocation);
        List<ShardRouting> assignedShards = allocation.routingNodes().assignedShards(shard.shardId());
        assertEquals(1, assignedShards.size());
        // the allocation result be consistent with allocation decision
        assertNotNull(allocateDecision.getTargetNode().getId(), assignedShards.get(0).currentNodeId());
    }

}
