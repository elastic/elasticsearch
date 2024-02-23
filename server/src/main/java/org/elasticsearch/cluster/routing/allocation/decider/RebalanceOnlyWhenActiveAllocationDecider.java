/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

/**
 * Only allow rebalancing when all shards are active within the shard replication group.
 */
public class RebalanceOnlyWhenActiveAllocationDecider extends AllocationDecider {

    public static final String NAME = "rebalance_only_when_active";

    static final Decision YES_ALL_REPLICAS_ACTIVE = Decision.single(
        Decision.Type.YES,
        NAME,
        "rebalancing is allowed as all copies of this shard are active"
    );
    static final Decision NO_SOME_REPLICAS_INACTIVE = Decision.single(
        Decision.Type.NO,
        NAME,
        "rebalancing is not allowed until all copies of this shard are active"
    );

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return allocation.routingNodes().allShardsActive(shardRouting.shardId(), allocation.metadata())
            ? YES_ALL_REPLICAS_ACTIVE
            : NO_SOME_REPLICAS_INACTIVE;
    }
}
