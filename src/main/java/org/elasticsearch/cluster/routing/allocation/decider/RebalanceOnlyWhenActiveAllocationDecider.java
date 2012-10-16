/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 * Only allow rebalancing when all shards are active within the shard replication group.
 */
public class RebalanceOnlyWhenActiveAllocationDecider extends AllocationDecider {

    @Inject
    public RebalanceOnlyWhenActiveAllocationDecider(Settings settings) {
        super(settings);
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        List<MutableShardRouting> shards = allocation.routingNodes().shardsRoutingFor(shardRouting);
        // its ok to check for active here, since in relocation, a shard is split into two in routing
        // nodes, once relocating, and one initializing
        for (int i = 0; i < shards.size(); i++) {
            if (!shards.get(i).active()) {
                return Decision.NO;
            }
        }
        return Decision.YES;
    }
}
