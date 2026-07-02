/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;

import java.util.Optional;

public class AllocateReshardSplitTargetPrimaryCommand extends BasePrimaryAllocationCommand {
    protected AllocateReshardSplitTargetPrimaryCommand(
        String index,
        int shardId,
        String node,
        boolean acceptDataLoss,
        ProjectId projectId
    ) {
        super(index, shardId, node, acceptDataLoss, projectId);
    }

    @Override
    public String name() {
        return "allocate_reshard_split_primary";
    }

    @Override
    public RerouteExplanation execute(RoutingAllocation allocation, boolean explain) {
        ShardRouting shardRouting;
        try {
            shardRouting = allocation.globalRoutingTable().routingTable(projectId).shardRoutingTable(index, shardId).primaryShard();
        } catch (IndexNotFoundException | ShardNotFoundException e) {
            return explainOrThrowRejectedCommand(explain, allocation, e);
        }

        if (shardRouting.unassigned() == false || shardRouting.recoverySource().getType() != RecoverySource.Type.EMPTY_STORE) {
            return explainOrThrowRejectedCommand(explain, allocation, "Requested shard is not in unassigned state");
        }

        Optional<IndexMetadata> maybeIndexMetadata = allocation.metadata().findIndex(shardRouting.index());
        if (maybeIndexMetadata.isEmpty()) {
            return explainOrThrowRejectedCommand(explain, allocation, "Requested index does not exist");
        }
        IndexMetadata indexMetadata = maybeIndexMetadata.get();

        if (indexMetadata.getReshardingMetadata() == null || indexMetadata.getReshardingMetadata().isSplit() == false) {
            return explainOrThrowRejectedCommand(explain, allocation, "Requested index is not being split");
        }

        if (indexMetadata.getReshardingMetadata().getSplit().isTargetShard(shardId) == false) {
            return explainOrThrowRejectedCommand(explain, allocation, "Requested shard is not a split target shard");
        }

        for (RoutingNodes.UnassignedShards.UnassignedIterator it = allocation.routingNodes().unassigned().iterator(); it.hasNext();) {
            ShardRouting unassigned = it.next();
            if (unassigned.equalsIgnoringMetadata(shardRouting) == false) {
                continue;
            }

            var unassignedInfo = new UnassignedInfo(
                UnassignedInfo.Reason.RESHARD_ADDED,
                "force allocation of resharding split target shard"
            );

            var recoverySource = new RecoverySource.ReshardSplitRecoverySource(
                new ShardId(shardRouting.index(), indexMetadata.getReshardingMetadata().getSplit().sourceShard(shardId))
            );

            it.updateUnassigned(unassignedInfo, recoverySource, allocation.changes());
            break;
        }

        return new RerouteExplanation(this, allocation.decision(Decision.YES, name() + " (allocation command)", "ignore deciders"));
    }
}
