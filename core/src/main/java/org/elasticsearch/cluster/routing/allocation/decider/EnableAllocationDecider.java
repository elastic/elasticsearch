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

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Locale;

/**
 * This allocation decider allows shard allocations / rebalancing via the cluster wide settings {@link #CLUSTER_ROUTING_ALLOCATION_ENABLE} /
 * {@link #CLUSTER_ROUTING_REBALANCE_ENABLE} and the per index setting {@link #INDEX_ROUTING_ALLOCATION_ENABLE} / {@link #INDEX_ROUTING_REBALANCE_ENABLE}.
 * The per index settings overrides the cluster wide setting.
 *
 * <p>
 * Allocation settings can have the following values (non-casesensitive):
 * <ul>
 *     <li> <code>NONE</code> - no shard allocation is allowed.
 *     <li> <code>NEW_PRIMARIES</code> - only primary shards of new indices are allowed to be allocated
 *     <li> <code>PRIMARIES</code> - only primary shards are allowed to be allocated
 *     <li> <code>ALL</code> - all shards are allowed to be allocated
 * </ul>
 * </p>
 *
 * <p>
 * Rebalancing settings can have the following values (non-casesensitive):
 * <ul>
 *     <li> <code>NONE</code> - no shard rebalancing is allowed.
 *     <li> <code>REPLICAS</code> - only replica shards are allowed to be balanced
 *     <li> <code>PRIMARIES</code> - only primary shards are allowed to be balanced
 *     <li> <code>ALL</code> - all shards are allowed to be balanced
 * </ul>
 * </p>
 *
 * @see Rebalance
 * @see Allocation
 */
public class EnableAllocationDecider extends AllocationDecider implements NodeSettingsService.Listener {

    public static final String NAME = "enable";

    public static final String CLUSTER_ROUTING_ALLOCATION_ENABLE = "cluster.routing.allocation.enable";
    public static final String INDEX_ROUTING_ALLOCATION_ENABLE = "index.routing.allocation.enable";

    public static final String CLUSTER_ROUTING_REBALANCE_ENABLE = "cluster.routing.rebalance.enable";
    public static final String INDEX_ROUTING_REBALANCE_ENABLE = "index.routing.rebalance.enable";

    private volatile Rebalance enableRebalance;
    private volatile Allocation enableAllocation;


    @Inject
    public EnableAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.enableAllocation = Allocation.parse(settings.get(CLUSTER_ROUTING_ALLOCATION_ENABLE, Allocation.ALL.name()));
        this.enableRebalance = Rebalance.parse(settings.get(CLUSTER_ROUTING_REBALANCE_ENABLE, Rebalance.ALL.name()));
        nodeSettingsService.addListener(this);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.ignoreDisable()) {
            return allocation.decision(Decision.YES, NAME, "allocation disabling is ignored");
        }

        Settings indexSettings = allocation.routingNodes().metaData().index(shardRouting.index()).settings();
        String enableIndexValue = indexSettings.get(INDEX_ROUTING_ALLOCATION_ENABLE);
        final Allocation enable;
        if (enableIndexValue != null) {
            enable = Allocation.parse(enableIndexValue);
        } else {
            enable = this.enableAllocation;
        }
        switch (enable) {
            case ALL:
                return allocation.decision(Decision.YES, NAME, "all allocations are allowed");
            case NONE:
                return allocation.decision(Decision.NO, NAME, "no allocations are allowed");
            case NEW_PRIMARIES:
                if (shardRouting.primary() && shardRouting.allocatedPostIndexCreate() == false) {
                    return allocation.decision(Decision.YES, NAME, "new primary allocations are allowed");
                } else {
                    return allocation.decision(Decision.NO, NAME, "non-new primary allocations are forbidden");
                }
            case PRIMARIES:
                if (shardRouting.primary()) {
                    return allocation.decision(Decision.YES, NAME, "primary allocations are allowed");
                } else {
                    return allocation.decision(Decision.NO, NAME, "replica allocations are forbidden");
                }
            default:
                throw new IllegalStateException("Unknown allocation option");
        }
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (allocation.ignoreDisable()) {
            return allocation.decision(Decision.YES, NAME, "rebalance disabling is ignored");
        }

        Settings indexSettings = allocation.routingNodes().metaData().index(shardRouting.index()).settings();
        String enableIndexValue = indexSettings.get(INDEX_ROUTING_REBALANCE_ENABLE);
        final Rebalance enable;
        if (enableIndexValue != null) {
            enable = Rebalance.parse(enableIndexValue);
        } else {
            enable = this.enableRebalance;
        }
        switch (enable) {
            case ALL:
                return allocation.decision(Decision.YES, NAME, "all rebalancing is allowed");
            case NONE:
                return allocation.decision(Decision.NO, NAME, "no rebalancing is allowed");
            case PRIMARIES:
                if (shardRouting.primary()) {
                    return allocation.decision(Decision.YES, NAME, "primary rebalancing is allowed");
                } else {
                    return allocation.decision(Decision.NO, NAME, "replica rebalancing is forbidden");
                }
            case REPLICAS:
                if (shardRouting.primary() == false) {
                    return allocation.decision(Decision.YES, NAME, "replica rebalancing is allowed");
                } else {
                    return allocation.decision(Decision.NO, NAME, "primary rebalancing is forbidden");
                }
            default:
                throw new IllegalStateException("Unknown rebalance option");
        }
    }

    @Override
    public void onRefreshSettings(Settings settings) {
        final Allocation enable = Allocation.parse(settings.get(CLUSTER_ROUTING_ALLOCATION_ENABLE, this.enableAllocation.name()));
        if (enable != this.enableAllocation) {
            logger.info("updating [{}] from [{}] to [{}]", CLUSTER_ROUTING_ALLOCATION_ENABLE, this.enableAllocation, enable);
            EnableAllocationDecider.this.enableAllocation = enable;
        }

        final Rebalance enableRebalance = Rebalance.parse(settings.get(CLUSTER_ROUTING_REBALANCE_ENABLE, this.enableRebalance.name()));
        if (enableRebalance != this.enableRebalance) {
            logger.info("updating [{}] from [{}] to [{}]", CLUSTER_ROUTING_REBALANCE_ENABLE, this.enableRebalance, enableRebalance);
            EnableAllocationDecider.this.enableRebalance = enableRebalance;
        }

    }

    /**
     * Allocation values or rather their string representation to be used used with
     * {@link EnableAllocationDecider#CLUSTER_ROUTING_ALLOCATION_ENABLE} / {@link EnableAllocationDecider#INDEX_ROUTING_ALLOCATION_ENABLE}
     * via cluster / index settings.
     */
    public enum Allocation {

        NONE,
        NEW_PRIMARIES,
        PRIMARIES,
        ALL;

        public static Allocation parse(String strValue) {
            if (strValue == null) {
                return null;
            } else {
                strValue = strValue.toUpperCase(Locale.ROOT);
                try {
                    return Allocation.valueOf(strValue);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Illegal allocation.enable value [" + strValue + "]");
                }
            }
        }
    }

    /**
     * Rebalance values or rather their string representation to be used used with
     * {@link EnableAllocationDecider#CLUSTER_ROUTING_REBALANCE_ENABLE} / {@link EnableAllocationDecider#INDEX_ROUTING_REBALANCE_ENABLE}
     * via cluster / index settings.
     */
    public enum Rebalance {

        NONE,
        PRIMARIES,
        REPLICAS,
        ALL;

        public static Rebalance parse(String strValue) {
            if (strValue == null) {
                return null;
            } else {
                strValue = strValue.toUpperCase(Locale.ROOT);
                try {
                    return Rebalance.valueOf(strValue);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Illegal rebalance.enable value [" + strValue + "]");
                }
            }
        }
    }

}
