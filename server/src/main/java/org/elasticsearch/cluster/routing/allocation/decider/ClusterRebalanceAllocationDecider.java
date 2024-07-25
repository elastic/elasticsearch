/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

import java.util.Locale;

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
    public static final Setting<ClusterRebalanceType> CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING = new Setting<>(
        CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE,
        settings -> ClusterModule.DESIRED_BALANCE_ALLOCATOR.equals(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.get(settings))
            ? ClusterRebalanceType.ALWAYS.toString()
            : ClusterRebalanceType.INDICES_ALL_ACTIVE.toString(),
        ClusterRebalanceType::parseString,
        Property.Dynamic,
        Property.NodeScope
    );

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
            throw new IllegalArgumentException(
                "Illegal value for " + CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING + ": " + typeString
            );
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private volatile ClusterRebalanceType type;

    public ClusterRebalanceAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING, this::setType);
        logger.debug("using [{}] with [{}]", CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, type);
    }

    private void setType(ClusterRebalanceType type) {
        this.type = type;
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return canRebalance(allocation);
    }

    private static final Decision YES_ALL_PRIMARIES_ACTIVE = Decision.single(Decision.Type.YES, NAME, "all primary shards are active");

    private static final Decision YES_ALL_SHARDS_ACTIVE = Decision.single(Decision.Type.YES, NAME, "all shards are active");

    private static final Decision NO_UNASSIGNED_PRIMARIES = Decision.single(
        Decision.Type.NO,
        NAME,
        "the cluster has unassigned primary shards and cluster setting ["
            + CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE
            + "] is set to ["
            + ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE
            + "]"
    );

    private static final Decision NO_INACTIVE_PRIMARIES = Decision.single(
        Decision.Type.NO,
        NAME,
        "the cluster has inactive primary shards and cluster setting ["
            + CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE
            + "] is set to ["
            + ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE
            + "]"
    );

    private static final Decision NO_UNASSIGNED_SHARDS = Decision.single(
        Decision.Type.NO,
        NAME,
        "the cluster has unassigned shards and cluster setting ["
            + CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE
            + "] is set to ["
            + ClusterRebalanceType.INDICES_ALL_ACTIVE
            + "]"
    );

    private static final Decision NO_INACTIVE_SHARDS = Decision.single(
        Decision.Type.NO,
        NAME,
        "the cluster has inactive shards and cluster setting ["
            + CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE
            + "] is set to ["
            + ClusterRebalanceType.INDICES_ALL_ACTIVE
            + "]"
    );

    @SuppressWarnings("fallthrough")
    @Override
    public Decision canRebalance(RoutingAllocation allocation) {
        final RoutingNodes routingNodes = allocation.routingNodes();
        switch (type) {
            case INDICES_PRIMARIES_ACTIVE:
                // check if there are unassigned primaries.
                if (routingNodes.hasUnassignedPrimaries()) {
                    return NO_UNASSIGNED_PRIMARIES;
                }
                // check if there are initializing primaries that don't have a relocatingNodeId entry.
                if (routingNodes.hasInactivePrimaries()) {
                    return NO_INACTIVE_PRIMARIES;
                }
                return YES_ALL_PRIMARIES_ACTIVE;
            case INDICES_ALL_ACTIVE:
                // check if there are unassigned shards.
                if (routingNodes.hasUnassignedShards()) {
                    return NO_UNASSIGNED_SHARDS;
                }
                // in case all indices are assigned, are there initializing shards which
                // are not relocating?
                if (routingNodes.hasInactiveShards()) {
                    return NO_INACTIVE_SHARDS;
                }
                // fall-through
            default:
                // all shards active from above or type == Type.ALWAYS
                return YES_ALL_SHARDS_ACTIVE;
        }
    }
}
