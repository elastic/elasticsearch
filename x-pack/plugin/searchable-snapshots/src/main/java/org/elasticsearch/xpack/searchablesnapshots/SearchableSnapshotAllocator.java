/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import java.util.ArrayList;
import java.util.List;

public class SearchableSnapshotAllocator implements ExistingShardsAllocator {

    public static final String ALLOCATOR_NAME = "searchable_snapshot_allocator";

    @Override
    public void beforeAllocation(RoutingAllocation allocation) {}

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        final AllocateUnassignedDecision allocateUnassignedDecision = decideAllocation(allocation, shardRouting);
        assert allocateUnassignedDecision.isDecisionTaken();

        if (allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.YES) {
            if (shardRouting.primary() && shardRouting.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE) {
                // we don't care what the allocation ID is since we know that these shards cannot really be stale, so we can
                // safely ignore the allocation ID with a forced-stale allocation and allow this shard to fall through to the balanced
                // shards allocator
                unassignedAllocationHandler.updateUnassigned(
                    shardRouting.unassignedInfo(),
                    RecoverySource.ExistingStoreRecoverySource.FORCE_STALE_PRIMARY_INSTANCE,
                    allocation.changes()
                );
            }
        } else {
            unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
        }
    }

    private static AllocateUnassignedDecision decideAllocation(RoutingAllocation allocation, ShardRouting shardRouting) {
        assert shardRouting.unassigned();
        assert ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.get(
            allocation.metadata().getIndexSafe(shardRouting.index()).getSettings()
        ).equals(ALLOCATOR_NAME);

        Decision.Type bestDecision = Decision.Type.NO;
        RoutingNode bestNode = null;
        final List<NodeAllocationResult> nodeAllocationResults = allocation.debugDecision()
            ? new ArrayList<>(allocation.routingNodes().size())
            : null;

        for (final RoutingNode routingNode : allocation.routingNodes()) {
            final Decision decision = allocation.deciders().canAllocate(shardRouting, routingNode, allocation);
            if (decision.type() == Decision.Type.YES || (decision.type() == Decision.Type.THROTTLE && bestDecision != Decision.Type.YES)) {
                bestDecision = decision.type();
                bestNode = routingNode;
            }
            if (nodeAllocationResults != null) {
                nodeAllocationResults.add(new NodeAllocationResult(routingNode.node(), null, decision));
            }
        }

        if (bestDecision == Decision.Type.YES) {
            return AllocateUnassignedDecision.yes(bestNode.node(), null, nodeAllocationResults, false);
        } else if (bestDecision == Decision.Type.THROTTLE) {
            return AllocateUnassignedDecision.throttle(nodeAllocationResults);
        } else {
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.DECIDERS_NO, nodeAllocationResults);
        }
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting shardRouting, RoutingAllocation routingAllocation) {
        assert shardRouting.unassigned();
        assert routingAllocation.debugDecision();
        return decideAllocation(routingAllocation, shardRouting);
    }

    @Override
    public void cleanCaches() {}

    @Override
    public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {}

    @Override
    public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {}

    @Override
    public int getNumberOfInFlightFetches() {
        return 0;
    }
}
