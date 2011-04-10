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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class ClusterRebalanceNodeAllocation extends NodeAllocation {

    public static enum ClusterRebalanceType {
        ALWAYS,
        INDICES_PRIMARIES_ACTIVE,
        INDICES_ALL_ACTIVE
    }

    private final ClusterRebalanceType type;

    @Inject public ClusterRebalanceNodeAllocation(Settings settings) {
        super(settings);
        String allowRebalance = componentSettings.get("allow_rebalance", "indices_all_active");
        if ("always".equalsIgnoreCase(allowRebalance)) {
            type = ClusterRebalanceType.ALWAYS;
        } else if ("indices_primaries_active".equalsIgnoreCase(allowRebalance) || "indicesPrimariesActive".equalsIgnoreCase(allowRebalance)) {
            type = ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE;
        } else if ("indices_all_active".equalsIgnoreCase(allowRebalance) || "indicesAllActive".equalsIgnoreCase(allowRebalance)) {
            type = ClusterRebalanceType.INDICES_ALL_ACTIVE;
        } else {
            logger.warn("[cluster.routing.allocation.allow_rebalance] has a wrong value {}, defaulting to 'indices_all_active'", allowRebalance);
            type = ClusterRebalanceType.INDICES_ALL_ACTIVE;
        }
        logger.debug("using [allow_rebalance] with [{}]", type.toString().toLowerCase());
    }

    @Override public boolean canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (type == ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE) {
            for (MutableShardRouting shard : allocation.routingNodes().unassigned()) {
                if (shard.primary()) {
                    return false;
                }
            }
            for (RoutingNode node : allocation.routingNodes()) {
                for (MutableShardRouting shard : node) {
                    if (shard.primary() && !shard.active()) {
                        return false;
                    }
                }
            }
            return true;
        }
        if (type == ClusterRebalanceType.INDICES_ALL_ACTIVE) {
            if (!allocation.routingNodes().unassigned().isEmpty()) {
                return false;
            }
            for (RoutingNode node : allocation.routingNodes()) {
                for (MutableShardRouting shard : node) {
                    if (!shard.active()) {
                        return false;
                    }
                }
            }
        }
        // type == Type.ALWAYS
        return true;
    }
}
