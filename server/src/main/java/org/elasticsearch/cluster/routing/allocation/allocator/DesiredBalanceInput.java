/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.List;

/**
 * The input to the desired balance computation.
 *
 * @param index             Each {@link DesiredBalanceInput} comes from a call to {@code reroute()} by a cluster state update, so they
 *                          arrive in sequence and newer inputs should supersede older ones. The {@link #index} of the input is its position
 *                          in this sequence.
 * @param routingAllocation a copy of (the immutable parts of) the context for the allocation decision process
 * @param ignoredShards     a list of the shards for which earlier allocators have claimed responsibility
 */
public record DesiredBalanceInput(long index, RoutingAllocation routingAllocation, List<ShardRouting> ignoredShards) {

    public static DesiredBalanceInput create(long index, RoutingAllocation routingAllocation) {
        return new DesiredBalanceInput(
            index,
            routingAllocation.immutableClone(),
            List.copyOf(routingAllocation.routingNodes().unassigned().ignored())
        );
    }
}
