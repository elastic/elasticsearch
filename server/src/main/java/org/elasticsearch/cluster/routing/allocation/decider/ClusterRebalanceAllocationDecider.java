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

import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

/**
 * This {@link AllocationDecider} controls re-balancing operations based on the
 * cluster wide active shard state. This decided can not be configured in
 * real-time and should be pre-cluster start via
 * {@code cluster.routing.allocation.allow_rebalance}. This setting respects the following
 * values:
 * <ul>
 * <li>{@code indices_primaries_active} - Re-balancing is allowed only once all
 * primary shards on all indices are active.</li>
 *
 * <li>{@code indices_all_active} - Re-balancing is allowed only once all
 * shards on all indices are active.</li>
 *
 * <li>{@code always} - Re-balancing is allowed once a shard replication group
 * is active</li>
 * </ul>
 */
public class ClusterRebalanceAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(ClusterRebalanceAllocationDecider.class);

    public static final String NAME = "cluster_rebalance";
    private static final String CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE = "cluster.routing.allocation.allow_rebalance";
    public static final Setting<ClusterRebalanceType> CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING =
        new Setting<>(CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, ClusterRebalanceType.INDICES_ALL_ACTIVE.toString(),
            ClusterRebalanceType::parseString, Property.Dynamic, Property.NodeScope);

    /**
     * An enum representation for the configured re-balance type.
     */
    public enum ClusterRebalanceType {
        /**
         * Re-balancing is allowed once a shard replication group is active
         */
        ALWAYS,
        /**
         * Re-balancing is allowed only once all primary shards on all indices are active.
         */
        INDICES_PRIMARIES_ACTIVE,
        /**
         * Re-balancing is allowed only once all shards on all indices are active.
         */
        INDICES_ALL_ACTIVE;

        public static ClusterRebalanceType parseString(String typeString) {
            if ("always".equalsIgnoreCase(typeString)) {
                return ClusterRebalanceType.ALWAYS;
            } else if ("indices_primaries_active".equalsIgnoreCase(typeString) || "indicesPrimariesActive".equalsIgnoreCase(typeString)) {
                return ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE;
            } else if ("indices_all_active".equalsIgnoreCase(typeString) || "indicesAllActive".equalsIgnoreCase(typeString)) {
                return ClusterRebalanceType.INDICES_ALL_ACTIVE;
            }
            throw new IllegalArgumentException("Illegal value for " +
                            CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING + ": " + typeString);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private volatile ClusterRebalanceType type;

    public ClusterRebalanceAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        try {
            type = CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.get(settings);
        } catch (IllegalStateException e) {
            logger.warn("[{}] has a wrong value {}, defaulting to 'indices_all_active'",
                    CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING,
                    CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getRaw(settings));
            type = ClusterRebalanceType.INDICES_ALL_ACTIVE;
        }
        logger.debug("using [{}] with [{}]", CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, type);

        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING, this::setType);
    }

    private void setType(ClusterRebalanceType type) {
        this.type = type;
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return canRebalance(allocation);
    }

    @Override
    public Decision canRebalance(RoutingAllocation allocation) {
        if (type == ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE) {
            // check if there are unassigned primaries.
            if ( allocation.routingNodes().hasUnassignedPrimaries() ) {
                return allocation.decision(Decision.NO, NAME,
                        "the cluster has unassigned primary shards and cluster setting [%s] is set to [%s]",
                        CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, type);
            }
            // check if there are initializing primaries that don't have a relocatingNodeId entry.
            if ( allocation.routingNodes().hasInactivePrimaries() ) {
                return allocation.decision(Decision.NO, NAME,
                        "the cluster has inactive primary shards and cluster setting [%s] is set to [%s]",
                        CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, type);
            }

            return allocation.decision(Decision.YES, NAME, "all primary shards are active");
        }
        if (type == ClusterRebalanceType.INDICES_ALL_ACTIVE) {
            // check if there are unassigned shards.
            if (allocation.routingNodes().hasUnassignedShards() ) {
                return allocation.decision(Decision.NO, NAME,
                        "the cluster has unassigned shards and cluster setting [%s] is set to [%s]",
                        CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, type);
            }
            // in case all indices are assigned, are there initializing shards which
            // are not relocating?
            if ( allocation.routingNodes().hasInactiveShards() ) {
                return allocation.decision(Decision.NO, NAME,
                        "the cluster has inactive shards and cluster setting [%s] is set to [%s]",
                        CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, type);
            }
        }
        // type == Type.ALWAYS
        return allocation.decision(Decision.YES, NAME, "all shards are active");
    }
}
