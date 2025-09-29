/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

/**
 * For an index of n shards hosted by a cluster of m nodes, a node should not host
 * significantly more than n / m shards. This allocation decider enforces this principle.
 * This allocation decider excludes any nodes flagged for shutdown from consideration
 * when computing optimal shard distributions.
 */
public class IndexShardCountAllocationDecider extends AllocationDecider {


    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Decision.NOT_PREFERRED;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.NOT_PREFERRED;
    }

}
