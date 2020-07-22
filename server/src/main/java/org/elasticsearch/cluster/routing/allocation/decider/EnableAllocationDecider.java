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

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

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

    public static final Setting<Allocation> CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING =
        new Setting<>("cluster.routing.allocation.enable", Allocation.ALL.toString(), Allocation::parse,
            Property.Dynamic, Property.NodeScope);
    public static final Setting<Allocation> INDEX_ROUTING_ALLOCATION_ENABLE_SETTING =
        new Setting<>("index.routing.allocation.enable", Allocation.ALL.toString(), Allocation::parse,
            Property.Dynamic, Property.IndexScope);

    public static final Setting<Rebalance> CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING =
        new Setting<>("cluster.routing.rebalance.enable", Rebalance.ALL.toString(), Rebalance::parse,
            Property.Dynamic, Property.NodeScope);
    public static final Setting<Rebalance> INDEX_ROUTING_REBALANCE_ENABLE_SETTING =
        new Setting<>("index.routing.rebalance.enable", Rebalance.ALL.toString(), Rebalance::parse,
            Property.Dynamic, Property.IndexScope);

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
            return allocation.decision(Decision.YES, NAME,
                "explicitly ignoring any disabling of allocation due to manual allocation commands via the reroute API");
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
        switch (enable) {
            case ALL:
                return allocation.decision(Decision.YES, NAME, "all allocations are allowed");
            case NONE:
                return allocation.decision(Decision.NO, NAME, "no allocations are allowed due to %s", setting(enable, usedIndexSetting));
            case NEW_PRIMARIES:
                if (shardRouting.primary() && shardRouting.active() == false &&
                    shardRouting.recoverySource().getType() != RecoverySource.Type.EXISTING_STORE) {
                    return allocation.decision(Decision.YES, NAME, "new primary allocations are allowed");
                } else {
                    return allocation.decision(Decision.NO, NAME, "non-new primary allocations are forbidden due to %s",
                                                setting(enable, usedIndexSetting));
                }
            case PRIMARIES:
                if (shardRouting.primary()) {
                    return allocation.decision(Decision.YES, NAME, "primary allocations are allowed");
                } else {
                    return allocation.decision(Decision.NO, NAME, "replica allocations are forbidden due to %s",
                                                setting(enable, usedIndexSetting));
                }
            default:
                throw new IllegalStateException("Unknown allocation option");
        }
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
        switch (enable) {
            case ALL:
                return allocation.decision(Decision.YES, NAME, "all rebalancing is allowed");
            case NONE:
                return allocation.decision(Decision.NO, NAME, "no rebalancing is allowed due to %s", setting(enable, usedIndexSetting));
            case PRIMARIES:
                if (shardRouting.primary()) {
                    return allocation.decision(Decision.YES, NAME, "primary rebalancing is allowed");
                } else {
                    return allocation.decision(Decision.NO, NAME, "replica rebalancing is forbidden due to %s",
                                                setting(enable, usedIndexSetting));
                }
            case REPLICAS:
                if (shardRouting.primary() == false) {
                    return allocation.decision(Decision.YES, NAME, "replica rebalancing is allowed");
                } else {
                    return allocation.decision(Decision.NO, NAME, "primary rebalancing is forbidden due to %s",
                                                setting(enable, usedIndexSetting));
                }
            default:
                throw new IllegalStateException("Unknown rebalance option");
        }
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

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

}
