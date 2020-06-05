/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

/**
 * Only allow rebalancing when all shards are active within the shard replication group.
 */
public class RebalanceOnlyWhenActiveAllocationDecider extends AllocationDecider {

    public static final String NAME = "rebalance_only_when_active";

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (!allocation.routingNodes().allReplicasActive(shardRouting.shardId(), allocation.metadata())) {
            return allocation.decision(Decision.NO, NAME, "rebalancing is not allowed until all replicas in the cluster are active");
        }
        return allocation.decision(Decision.YES, NAME, "rebalancing is allowed as all replicas are active in the cluster");
    }
}
