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

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
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

    public static final Setting<Allocation> CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING =
        new Setting<>("cluster.routing.allocation.enable", Allocation.ALL.name(), Allocation::parse,
            Property.Dynamic, Property.NodeScope);
    public static final Setting<Allocation> INDEX_ROUTING_ALLOCATION_ENABLE_SETTING =
        new Setting<>("index.routing.allocation.enable", Allocation.ALL.name(), Allocation::parse,
            Property.Dynamic, Property.IndexScope);

    public static final Setting<Rebalance> CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING =
        new Setting<>("cluster.routing.rebalance.enable", Rebalance.ALL.name(), Rebalance::parse,
            Property.Dynamic, Property.NodeScope);
    public static final Setting<Rebalance> INDEX_ROUTING_REBALANCE_ENABLE_SETTING =
        new Setting<>("index.routing.rebalance.enable", Rebalance.ALL.name(), Rebalance::parse,
            Property.Dynamic, Property.IndexScope);

    private volatile Rebalance enableRebalance;
    private volatile Allocation enableAllocation;

    @Inject
    public EnableAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        this.enableAllocation = CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.get(settings);
        this.enableRebalance = CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING, this::setEnableAllocation);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING, this::setEnableRebalance);
    }

    public void setEnableRebalance(Rebalance enableRebalance) {
        this.enableRebalance = enableRebalance;
    }

    public void setEnableAllocation(Allocation enableAllocation) {
        this.enableAllocation = enableAllocation;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.ignoreDisable()) {
            return allocation.decision(Decision.YES, NAME, "allocation is explicitly ignoring any disabling of allocation");
        }

        final IndexMetaData indexMetaData = allocation.metaData().getIndexSafe(shardRouting.index());
        final Allocation enable;
        if (INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.exists(indexMetaData.getSettings())) {
            enable = INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.get(indexMetaData.getSettings());
        } else {
            enable = this.enableAllocation;
        }
        switch (enable) {
            case ALL:
                return allocation.decision(Decision.YES, NAME, "all allocations are allowed");
            case NONE:
                return allocation.decision(Decision.NO, NAME, "no allocations are allowed");
            case NEW_PRIMARIES:
                if (shardRouting.primary() && shardRouting.allocatedPostIndexCreate(indexMetaData) == false) {
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
            return allocation.decision(Decision.YES, NAME, "allocation is explicitly ignoring any disabling of relocation");
        }

        Settings indexSettings = allocation.metaData().getIndexSafe(shardRouting.index()).getSettings();
        final Rebalance enable;
        if (INDEX_ROUTING_REBALANCE_ENABLE_SETTING.exists(indexSettings)) {
            enable = INDEX_ROUTING_REBALANCE_ENABLE_SETTING.get(indexSettings);
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
    }

}
