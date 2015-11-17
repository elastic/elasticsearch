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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Locale;

/**
 * This {@link AllocationDecider} controls re-balancing operations based on the
 * cluster wide active shard state. This decided can not be configured in
 * real-time and should be pre-cluster start via
 * <tt>cluster.routing.allocation.allow_rebalance</tt>. This setting respects the following
 * values:
 * <ul>
 * <li><tt>indices_primaries_active</tt> - Re-balancing is allowed only once all
 * primary shards on all indices are active.</li>
 * 
 * <li><tt>indices_all_active</tt> - Re-balancing is allowed only once all
 * shards on all indices are active.</li>
 * 
 * <li><tt>always</tt> - Re-balancing is allowed once a shard replication group
 * is active</li>
 * </ul>
 */
public class ClusterRebalanceAllocationDecider extends AllocationDecider {

    public static final String NAME = "cluster_rebalance";

    public static final String CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE = "cluster.routing.allocation.allow_rebalance";
    public static final Validator ALLOCATION_ALLOW_REBALANCE_VALIDATOR = (setting, value, clusterState) -> {
        try {
            ClusterRebalanceType.parseString(value);
            return null;
        } catch (IllegalArgumentException e) {
            return "the value of " + setting + " must be one of: [always, indices_primaries_active, indices_all_active]";
        }
    };

    /**
     * An enum representation for the configured re-balance type. 
     */
    public static enum ClusterRebalanceType {
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
            throw new IllegalArgumentException("Illegal value for " + CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE + ": " + typeString);
        }
    }

    private ClusterRebalanceType type;

    @Inject
    public ClusterRebalanceAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        String allowRebalance = settings.get(CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "indices_all_active");
        try {
            type = ClusterRebalanceType.parseString(allowRebalance);
        } catch (IllegalStateException e) {
            logger.warn("[{}] has a wrong value {}, defaulting to 'indices_all_active'", CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, allowRebalance);
            type = ClusterRebalanceType.INDICES_ALL_ACTIVE;
        }
        logger.debug("using [{}] with [{}]", CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, type.toString().toLowerCase(Locale.ROOT));

        nodeSettingsService.addListener(new ApplySettings());
    }

    class ApplySettings implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            String newAllowRebalance = settings.get(CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, null);
            if (newAllowRebalance != null) {
                ClusterRebalanceType newType = null;
                try {
                    newType = ClusterRebalanceType.parseString(newAllowRebalance);
                } catch (IllegalArgumentException e) {
                    // ignore
                }

                if (newType != null && newType != ClusterRebalanceAllocationDecider.this.type) {
                    logger.info("updating [{}] from [{}] to [{}]", CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE,
                            ClusterRebalanceAllocationDecider.this.type.toString().toLowerCase(Locale.ROOT),
                            newType.toString().toLowerCase(Locale.ROOT));
                    ClusterRebalanceAllocationDecider.this.type = newType;
                }
            }
        }
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
                return allocation.decision(Decision.NO, NAME, "cluster has unassigned primary shards");
            }
            // check if there are initializing primaries that don't have a relocatingNodeId entry.
            if ( allocation.routingNodes().hasInactivePrimaries() ) {
                return allocation.decision(Decision.NO, NAME, "cluster has inactive primary shards");
            }

            return allocation.decision(Decision.YES, NAME, "all primary shards are active");
        }
        if (type == ClusterRebalanceType.INDICES_ALL_ACTIVE) {
            // check if there are unassigned shards.
            if (allocation.routingNodes().hasUnassignedShards() ) {
                return allocation.decision(Decision.NO, NAME, "cluster has unassigned shards");
            }
            // in case all indices are assigned, are there initializing shards which
            // are not relocating?
            if ( allocation.routingNodes().hasInactiveShards() ) {
                return allocation.decision(Decision.NO, NAME, "cluster has inactive shards");
            }
        }
        // type == Type.ALWAYS
        return allocation.decision(Decision.YES, NAME, "all shards are active");
    }
}
