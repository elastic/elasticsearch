/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class ConcurrentRebalanceNodeAllocation extends NodeAllocation {

    private final int clusterConcurrentRebalance;

    @Inject public ConcurrentRebalanceNodeAllocation(Settings settings) {
        super(settings);
        this.clusterConcurrentRebalance = componentSettings.getAsInt("cluster_concurrent_rebalance", 2);
        logger.debug("using [cluster_concurrent_rebalance] with [{}]", clusterConcurrentRebalance);
    }

    @Override public boolean canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (clusterConcurrentRebalance == -1) {
            return true;
        }
        int rebalance = 0;
        for (RoutingNode node : allocation.routingNodes()) {
            for (MutableShardRouting shard : node) {
                if (shard.state() == ShardRoutingState.RELOCATING) {
                    rebalance++;
                }
            }
        }
        if (rebalance >= clusterConcurrentRebalance) {
            return false;
        }
        return true;
    }
}