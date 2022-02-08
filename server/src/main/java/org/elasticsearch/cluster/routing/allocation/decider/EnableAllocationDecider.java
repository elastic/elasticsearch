/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;

/**
 * This allocation decider allows shard allocations / rebalancing via the cluster wide settings
 * {@link #CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING} / {@link #CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING} and the per index setting
 * {@link #INDEX_ROUTING_ALLOCATION_ENABLE_SETTING} / {@link #INDEX_ROUTING_REBALANCE_ENABLE_SETTING}.
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
 *
 * <p>
 * Rebalancing settings can have the following values (non-casesensitive):
 * <ul>
 *     <li> <code>NONE</code> - no shard rebalancing is allowed.
 *     <li> <code>REPLICAS</code> - only replica shards are allowed to be balanced
 *     <li> <code>PRIMARIES</code> - only primary shards are allowed to be balanced
 *     <li> <code>ALL</code> - all shards are allowed to be balanced
 * </ul>
 *
 * @see Rebalance
 * @see Allocation
 */
public class EnableAllocationDecider extends AllocationDecider {

    public static final String NAME = "enable";

    public static final Setting<Allocation> CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING = Setting.enumSetting(
        Allocation.class,
        "cluster.routing.allocation.enable",
        Allocation.ALL,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Allocation> INDEX_ROUTING_ALLOCATION_ENABLE_SETTING = Setting.enumSetting(
        Allocation.class,
        "index.routing.allocation.enable",
        Allocation.ALL,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Rebalance> CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING = Setting.enumSetting(
        Rebalance.class,
        "cluster.routing.rebalance.enable",
        Rebalance.ALL,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Rebalance> INDEX_ROUTING_REBALANCE_ENABLE_SETTING = Setting.enumSetting(
        Rebalance.class,
        "index.routing.rebalance.enable",
        Rebalance.ALL,
        Property.Dynamic,
        Property.IndexScope
    );

    private volatile Rebalance enableRebalance;
    private volatile Allocation enableAllocation;

    public EnableAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.enableAllocation = CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.get(settings);
        this.enableRebalance = CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING, this::setEnableAllocation);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING, this::setEnableRebalance);
    }

    private void setEnableRebalance(Rebalance enableRebalance) {
        this.enableRebalance = enableRebalance;
    }

    private void setEnableAllocation(Allocation enableAllocation) {
        this.enableAllocation = enableAllocation;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (allocation.ignoreDisable()) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "explicitly ignoring any disabling of allocation due to manual allocation commands via the reroute API"
            );
        }

        final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        final Allocation enable;
        final boolean usedIndexSetting;
        if (INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.exists(indexMetadata.getSettings())) {
            enable = INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.get(indexMetadata.getSettings());
            usedIndexSetting = true;
        } else {
            enable = this.enableAllocation;
            usedIndexSetting = false;
        }
        return switch (enable) {
            case ALL -> allocation.decision(Decision.YES, NAME, "all allocations are allowed");
            case NONE -> allocation.decision(Decision.NO, NAME, "no allocations are allowed due to %s", setting(enable, usedIndexSetting));
            case NEW_PRIMARIES -> (shardRouting.primary()
                && shardRouting.active() == false
                && shardRouting.recoverySource().getType() != RecoverySource.Type.EXISTING_STORE)
                    ? allocation.decision(Decision.YES, NAME, "new primary allocations are allowed")
                    : allocation.decision(
                        Decision.NO,
                        NAME,
                        "non-new primary allocations are forbidden due to %s",
                        setting(enable, usedIndexSetting)
                    );
            case PRIMARIES -> shardRouting.primary()
                ? allocation.decision(Decision.YES, NAME, "primary allocations are allowed")
                : allocation.decision(Decision.NO, NAME, "replica allocations are forbidden due to %s", setting(enable, usedIndexSetting));
        };
    }

    @Override
    public Decision canRebalance(RoutingAllocation allocation) {
        if (allocation.ignoreDisable()) {
            return allocation.decision(Decision.YES, NAME, "allocation is explicitly ignoring any disabling of rebalancing");
        }

        if (enableRebalance == Rebalance.NONE) {
            for (IndexMetadata indexMetadata : allocation.metadata()) {
                if (INDEX_ROUTING_REBALANCE_ENABLE_SETTING.exists(indexMetadata.getSettings())
                    && INDEX_ROUTING_REBALANCE_ENABLE_SETTING.get(indexMetadata.getSettings()) != Rebalance.NONE) {
                    return allocation.decision(Decision.YES, NAME, "rebalancing is permitted on one or more indices");
                }
            }
            return allocation.decision(Decision.NO, NAME, "no rebalancing is allowed due to %s", setting(enableRebalance, false));
        }

        return allocation.decision(Decision.YES, NAME, "rebalancing is not globally disabled");
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (allocation.ignoreDisable()) {
            return allocation.decision(Decision.YES, NAME, "allocation is explicitly ignoring any disabling of rebalancing");
        }

        Settings indexSettings = allocation.metadata().getIndexSafe(shardRouting.index()).getSettings();
        final Rebalance enable;
        final boolean usedIndexSetting;
        if (INDEX_ROUTING_REBALANCE_ENABLE_SETTING.exists(indexSettings)) {
            enable = INDEX_ROUTING_REBALANCE_ENABLE_SETTING.get(indexSettings);
            usedIndexSetting = true;
        } else {
            enable = this.enableRebalance;
            usedIndexSetting = false;
        }
        return switch (enable) {
            case ALL -> allocation.decision(Decision.YES, NAME, "all rebalancing is allowed");
            case NONE -> allocation.decision(Decision.NO, NAME, "no rebalancing is allowed due to %s", setting(enable, usedIndexSetting));
            case PRIMARIES -> shardRouting.primary()
                ? allocation.decision(Decision.YES, NAME, "primary rebalancing is allowed")
                : allocation.decision(Decision.NO, NAME, "replica rebalancing is forbidden due to %s", setting(enable, usedIndexSetting));
            case REPLICAS -> shardRouting.primary()
                ? allocation.decision(Decision.NO, NAME, "primary rebalancing is forbidden due to %s", setting(enable, usedIndexSetting))
                : allocation.decision(Decision.YES, NAME, "replica rebalancing is allowed");
        };
    }

    private static String setting(Allocation allocation, boolean usedIndexSetting) {
        StringBuilder buf = new StringBuilder();
        if (usedIndexSetting) {
            buf.append("index setting [");
            buf.append(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey());
        } else {
            buf.append("cluster setting [");
            buf.append(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey());
        }
        buf.append("=").append(allocation.toString().toLowerCase(Locale.ROOT)).append("]");
        return buf.toString();
    }

    private static String setting(Rebalance rebalance, boolean usedIndexSetting) {
        StringBuilder buf = new StringBuilder();
        if (usedIndexSetting) {
            buf.append("index setting [");
            buf.append(INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey());
        } else {
            buf.append("cluster setting [");
            buf.append(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey());
        }
        buf.append("=").append(rebalance.toString().toLowerCase(Locale.ROOT)).append("]");
        return buf.toString();
    }

    /**
     * Allocation values or rather their string representation to be used used with
     * {@link EnableAllocationDecider#CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING} /
     * {@link EnableAllocationDecider#INDEX_ROUTING_ALLOCATION_ENABLE_SETTING}
     * via cluster / index settings.
     */
    public enum Allocation {

        NONE,
        NEW_PRIMARIES,
        PRIMARIES,
        ALL;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Rebalance values or rather their string representation to be used used with
     * {@link EnableAllocationDecider#CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING} /
     * {@link EnableAllocationDecider#INDEX_ROUTING_REBALANCE_ENABLE_SETTING}
     * via cluster / index settings.
     */
    public enum Rebalance {

        NONE,
        PRIMARIES,
        REPLICAS,
        ALL;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

}
