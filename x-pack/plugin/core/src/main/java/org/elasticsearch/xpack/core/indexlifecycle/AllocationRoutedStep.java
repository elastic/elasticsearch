/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.index.Index;

import java.util.List;

public class AllocationRoutedStep extends ClusterStateWaitStep {
    private static final Logger logger = ESLoggerFactory.getLogger(AllocationRoutedStep.class);

    private AllocationDeciders allocationDeciders;

    AllocationRoutedStep(StepKey key, StepKey nextStepKey, AllocationDeciders allocationDeciders) {
        super(key, nextStepKey);
        this.allocationDeciders = allocationDeciders;
    }

    @Override
    public boolean isConditionMet(Index index, ClusterState clusterState) {
        // All the allocation attributes are already set so just need to check if the allocation has happened
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState, null,
            System.nanoTime());
        int allocationPendingShards = 0;
        List<ShardRouting> allShards = clusterState.getRoutingTable().allShards(index.getName());
        for (ShardRouting shardRouting : allShards) {
            assert shardRouting.active() : "Shard not active, found " + shardRouting.state() + "for shard with id: "
                + shardRouting.shardId();
            String currentNodeId = shardRouting.currentNodeId();
            boolean canRemainOnCurrentNode = allocationDeciders.canRemain(shardRouting,
                clusterState.getRoutingNodes().node(currentNodeId), allocation).type() == Decision.Type.YES;
            if (canRemainOnCurrentNode == false) {
                allocationPendingShards++;
            }
        }
        if (allocationPendingShards > 0) {
            logger.debug("[{}] lifecycle action for index [{}] waiting for [{}] shards "
                + "to be allocated to nodes matching the given filters", getKey().getAction(), index, allocationPendingShards);
            return false;
        } else {
            logger.debug("[{}] lifecycle action for index [{}] complete", getKey().getAction(), index);
            return true;
        }
    }
}
