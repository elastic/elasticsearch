/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * The input to the desired balance computation.
 *
 * @param index             Each {@link DesiredBalanceInput} comes from a call to {@code reroute()} by a cluster state update, so they
 *                          arrive in sequence and newer inputs should supersede older ones. The {@link #index} of the input is its position
 *                          in this sequence.
 * @param routingAllocation a copy of (the immutable parts of) the context for the allocation decision process
 * @param ignoredShards     a list of the shards for which earlier allocators have claimed responsibility
 */
public record DesiredBalanceInput(long index, RoutingAllocation routingAllocation, Set<ShardRouting> ignoredShards) {

    public static DesiredBalanceInput create(long index, RoutingAllocation routingAllocation) {
        return new DesiredBalanceInput(
            index,
            routingAllocation.immutableClone(),
            getIgnoredShardsWithDiscardedAllocationStatus(routingAllocation)
        );
    }

    private static Set<ShardRouting> getIgnoredShardsWithDiscardedAllocationStatus(RoutingAllocation routingAllocation) {
        return routingAllocation.routingNodes()
            .unassigned()
            .ignored()
            .stream()
            .flatMap(
                shardRouting -> Stream.of(
                    shardRouting,
                    shardRouting.updateUnassigned(discardAllocationStatus(shardRouting.unassignedInfo()), shardRouting.recoverySource())
                )
            )
            .collect(toUnmodifiableSet());
    }

    /**
     * AllocationStatus is discarded as it might come from GatewayAllocator and not be present in corresponding routing table
     */
    private static UnassignedInfo discardAllocationStatus(UnassignedInfo info) {
        return new UnassignedInfo(
            info.getReason(),
            info.getMessage(),
            info.getFailure(),
            info.getNumFailedAllocations(),
            info.getUnassignedTimeInNanos(),
            info.getUnassignedTimeInMillis(),
            info.isDelayed(),
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            info.getFailedNodeIds(),
            info.getLastAllocatedNodeId()
        );
    }
}
