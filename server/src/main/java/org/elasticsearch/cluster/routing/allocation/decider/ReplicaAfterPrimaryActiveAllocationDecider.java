/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

/**
 * An allocation strategy that only allows for a replica to be allocated when the primary is active.
 */
public class ReplicaAfterPrimaryActiveAllocationDecider extends AllocationDecider {

    private static final String NAME = "replica_after_primary_active";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            return allocation.decision(Decision.YES, NAME, "shard is primary and can be allocated");
        }
        ShardRouting primary = allocation.routingNodes().activePrimary(shardRouting.shardId());
        if (primary == null) {
            return allocation.decision(Decision.NO, NAME, "primary shard for this replica is not yet active");
        }
        return allocation.decision(Decision.YES, NAME, "primary shard for this replica is already active");
    }
}
