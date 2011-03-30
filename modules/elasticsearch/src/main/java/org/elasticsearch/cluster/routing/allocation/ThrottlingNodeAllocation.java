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

/**
 * @author kimchy (shay.banon)
 */
public class ThrottlingNodeAllocation extends NodeAllocation {

    private final int primariesInitialRecoveries;
    private final int concurrentRecoveries;

    @Inject public ThrottlingNodeAllocation(Settings settings) {
        super(settings);

        this.primariesInitialRecoveries = componentSettings.getAsInt("node_initial_primaries_recoveries", 4);
        this.concurrentRecoveries = componentSettings.getAsInt("concurrent_recoveries", componentSettings.getAsInt("node_concurrent_recoveries", 2));
        logger.debug("using node_concurrent_recoveries [{}], node_initial_primaries_recoveries [{}]", concurrentRecoveries, primariesInitialRecoveries);
    }

    @Override public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            boolean primaryUnassigned = false;
            for (MutableShardRouting shard : allocation.routingNodes().unassigned()) {
                if (shard.shardId().equals(shardRouting.shardId())) {
                    primaryUnassigned = true;
                }
            }
            if (primaryUnassigned) {
                // primary is unassigned, means we are going to do recovery from gateway
                // count *just the primary* currently doing recovery on the node and check against concurrent_recoveries
                int primariesInRecovery = 0;
                for (MutableShardRouting shard : node) {
                    if (shard.state() == ShardRoutingState.INITIALIZING && shard.primary()) {
                        primariesInRecovery++;
                    }
                }
                if (primariesInRecovery >= primariesInitialRecoveries) {
                    return Decision.THROTTLE;
                } else {
                    return Decision.YES;
                }
            }
        }

        // either primary or replica doing recovery (from peer shard)

        // count the number of recoveries on the node, its for both target (INITIALIZING) and source (RELOCATING)
        int currentRecoveries = 0;
        for (MutableShardRouting shard : node) {
            if (shard.state() == ShardRoutingState.INITIALIZING || shard.state() == ShardRoutingState.RELOCATING) {
                currentRecoveries++;
            }
        }

        if (currentRecoveries >= concurrentRecoveries) {
            return Decision.THROTTLE;
        } else {
            return Decision.YES;
        }
    }
}
