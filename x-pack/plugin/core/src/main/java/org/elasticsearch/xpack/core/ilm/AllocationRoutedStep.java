/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Collections;

import static org.elasticsearch.xpack.core.ilm.step.info.AllocationInfo.allShardsActiveAllocationInfo;
import static org.elasticsearch.xpack.core.ilm.step.info.AllocationInfo.waitingForActiveShardsAllocationInfo;

/**
 * Checks whether all shards have been correctly routed in response to an update to the allocation rules for an index.
 */
public class AllocationRoutedStep extends ClusterStateWaitStep {
    public static final String NAME = "check-allocation";

    private static final Logger logger = LogManager.getLogger(AllocationRoutedStep.class);

    AllocationRoutedStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetadata idxMeta = clusterState.metadata().index(index);
        if (idxMeta == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            return new Result(false, null);
        }
        if (ActiveShardCount.ALL.enoughShardsActive(clusterState, index.getName()) == false) {
            logger.debug(
                "[{}] lifecycle action for index [{}] cannot make progress because not all shards are active",
                getKey().getAction(),
                index.getName()
            );
            return new Result(false, waitingForActiveShardsAllocationInfo(idxMeta.getNumberOfReplicas()));
        }

        AllocationDeciders allocationDeciders = new AllocationDeciders(
            Collections.singletonList(
                new FilterAllocationDecider(
                    clusterState.getMetadata().settings(),
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                )
            )
        );
        int allocationPendingAllShards = getPendingAllocations(index, allocationDeciders, clusterState);

        if (allocationPendingAllShards > 0) {
            logger.debug(
                "{} lifecycle action [{}] waiting for [{}] shards to be allocated to nodes matching the given filters",
                index,
                getKey().getAction(),
                allocationPendingAllShards
            );
            return new Result(false, allShardsActiveAllocationInfo(idxMeta.getNumberOfReplicas(), allocationPendingAllShards));
        } else {
            logger.debug("{} lifecycle action for [{}] complete", index, getKey().getAction());
            return new Result(true, null);
        }
    }

    static int getPendingAllocations(Index index, AllocationDeciders allocationDeciders, ClusterState clusterState) {
        // All the allocation attributes are already set so just need to check
        // if the allocation has happened
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState, null, null, System.nanoTime());

        int allocationPendingAllShards = 0;

        final IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(index);
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            final IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = indexShardRoutingTable.shard(copy);
                String currentNodeId = shardRouting.currentNodeId();
                boolean canRemainOnCurrentNode = allocationDeciders.canRemain(
                    shardRouting,
                    clusterState.getRoutingNodes().node(currentNodeId),
                    allocation
                ).type() == Decision.Type.YES;
                if (canRemainOnCurrentNode == false || shardRouting.started() == false) {
                    allocationPendingAllShards++;
                }
            }
        }
        return allocationPendingAllShards;
    }
}
