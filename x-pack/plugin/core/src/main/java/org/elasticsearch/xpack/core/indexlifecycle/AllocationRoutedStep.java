/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Collections;
import java.util.List;

public class AllocationRoutedStep extends ClusterStateWaitStep {
    public static final String NAME = "check-allocation";

    private static final Logger logger = ESLoggerFactory.getLogger(AllocationRoutedStep.class);

    private static final AllocationDeciders ALLOCATION_DECIDERS = new AllocationDeciders(Settings.EMPTY, Collections.singletonList(
            new FilterAllocationDecider(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))));

    AllocationRoutedStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isConditionMet(Index index, ClusterState clusterState) {
        if (ActiveShardCount.ALL.enoughShardsActive(clusterState, index.getName()) == false) {
            logger.debug("[{}] lifecycle action for index [{}] cannot make progress because not all shards are active",
                    getKey().getAction(), index.getName());
            return false;
        }
        IndexMetaData idxMeta = clusterState.metaData().index(index);
        if (idxMeta == null) {
            throw new IndexNotFoundException("Index not found when executing " + getKey().getAction() + " lifecycle action.",
                    index.getName());
        }
        // All the allocation attributes are already set so just need to check
        // if the allocation has happened
        RoutingAllocation allocation = new RoutingAllocation(ALLOCATION_DECIDERS, clusterState.getRoutingNodes(), clusterState, null,
                System.nanoTime());
        int allocationPendingShards = 0;
        List<ShardRouting> allShards = clusterState.getRoutingTable().allShards(index.getName());
        for (ShardRouting shardRouting : allShards) {
            String currentNodeId = shardRouting.currentNodeId();
            boolean canRemainOnCurrentNode = ALLOCATION_DECIDERS
                    .canRemain(shardRouting, clusterState.getRoutingNodes().node(currentNodeId), allocation).type() == Decision.Type.YES;
            if (canRemainOnCurrentNode == false) {
                allocationPendingShards++;
            }
        }
        if (allocationPendingShards > 0) {
            logger.debug(
                    "[{}] lifecycle action for index [{}] waiting for [{}] shards " + "to be allocated to nodes matching the given filters",
                    getKey().getAction(), index, allocationPendingShards);
            return false;
        } else {
            logger.debug("[{}] lifecycle action for index [{}] complete", getKey().getAction(), index);
            return true;
        }
    }
}
