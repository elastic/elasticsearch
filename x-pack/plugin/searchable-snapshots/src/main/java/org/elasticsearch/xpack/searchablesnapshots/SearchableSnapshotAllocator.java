/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;

public class SearchableSnapshotAllocator implements ExistingShardsAllocator {

    static final String ALLOCATOR_NAME = "searchable_snapshot_allocator";

    @Override
    public void allocateUnassigned(RoutingAllocation allocation) {

        final RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();

        while (iterator.hasNext()) {
            final ShardRouting shardRouting = iterator.next();
            final AllocateUnassignedDecision allocateUnassignedDecision = decideAllocation(allocation, shardRouting);

            if (allocateUnassignedDecision.isDecisionTaken()) {
                if (allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.YES) {
                    if (shardRouting.primary() && shardRouting.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE) {
                        // we don't care what the allocation ID is since we know that these shards cannot really be stale, so we can
                        // safely ignore the allocation ID with a forced-stale allocation
                        iterator.updateUnassigned(shardRouting.unassignedInfo(),
                            RecoverySource.ExistingStoreRecoverySource.FORCE_STALE_PRIMARY_INSTANCE, allocation.changes());
                    }
                    iterator.initialize(allocateUnassignedDecision.getTargetNode().getId(), null, 0L, allocation.changes());
                } else {
                    iterator.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
                }
            }
        }
    }

    private static AllocateUnassignedDecision decideAllocation(RoutingAllocation allocation, ShardRouting shardRouting) {
        if (isResponsibleFor(allocation, shardRouting)) {
            Decision.Type bestDecision = Decision.Type.NO;
            RoutingNode bestNode = null;
            final List<NodeAllocationResult> nodeAllocationResults
                = allocation.debugDecision() ? new ArrayList<>(allocation.routingNodes().size()) : null;

            for (final RoutingNode routingNode : allocation.routingNodes()) {
                final Decision decision = allocation.deciders().canAllocate(shardRouting, routingNode, allocation);
                if (decision.type() == Decision.Type.YES
                    || (decision.type() == Decision.Type.THROTTLE && bestDecision != Decision.Type.YES)) {
                    bestDecision = decision.type();
                    bestNode = routingNode;
                }
                if (nodeAllocationResults != null) {
                    nodeAllocationResults.add(new NodeAllocationResult(routingNode.node(), null, decision));
                }
            }

            switch (bestDecision) {
                case YES:
                    return AllocateUnassignedDecision.yes(bestNode.node(), null, nodeAllocationResults, false);
                case THROTTLE:
                    return AllocateUnassignedDecision.throttle(nodeAllocationResults);
                case NO:
                    return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.DECIDERS_NO, nodeAllocationResults);
            }
        }

        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    private static boolean isResponsibleFor(RoutingAllocation allocation, ShardRouting shardRouting) {
        final Settings settings = allocation.metaData().getIndexSafe(shardRouting.index()).getSettings();
        return shardRouting.unassigned() && ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.get(settings).equals(ALLOCATOR_NAME);
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting shardRouting, RoutingAllocation routingAllocation) {
        assert shardRouting.unassigned();
        assert routingAllocation.debugDecision();
        return decideAllocation(routingAllocation, shardRouting);
    }

    @Override
    public void cleanCaches() {

    }

    @Override
    public void applyStartedShards(RoutingAllocation allocation, List<ShardRouting> startedShards) {

    }

    @Override
    public void applyFailedShards(RoutingAllocation allocation, List<FailedShard> failedShards) {

    }

    @Override
    public int getNumberOfInFlightFetches() {
        return 0;
    }
}
