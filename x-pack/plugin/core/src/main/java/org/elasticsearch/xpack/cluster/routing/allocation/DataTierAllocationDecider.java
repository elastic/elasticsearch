/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.DataTier;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The {@code DataTierAllocationDecider} is a custom allocation decider that behaves similar to the
 * {@link org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider}, however it
 * is specific to the {@code _tier} setting for both the cluster and index level.
 */
public class DataTierAllocationDecider extends AllocationDecider {

    public static final String NAME = "data_tier";

    public static final String CLUSTER_ROUTING_REQUIRE = "cluster.routing.allocation.require._tier";
    public static final String CLUSTER_ROUTING_INCLUDE = "cluster.routing.allocation.include._tier";
    public static final String CLUSTER_ROUTING_EXCLUDE = "cluster.routing.allocation.exclude._tier";
    public static final String INDEX_ROUTING_REQUIRE = "index.routing.allocation.require._tier";
    public static final String INDEX_ROUTING_INCLUDE = "index.routing.allocation.include._tier";
    public static final String INDEX_ROUTING_EXCLUDE = "index.routing.allocation.exclude._tier";

    public static final Setting<String> CLUSTER_ROUTING_REQUIRE_SETTING = Setting.simpleString(CLUSTER_ROUTING_REQUIRE,
        DataTierAllocationDecider::validateTierSetting, Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> CLUSTER_ROUTING_INCLUDE_SETTING = Setting.simpleString(CLUSTER_ROUTING_INCLUDE,
        DataTierAllocationDecider::validateTierSetting, Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> CLUSTER_ROUTING_EXCLUDE_SETTING = Setting.simpleString(CLUSTER_ROUTING_EXCLUDE,
        DataTierAllocationDecider::validateTierSetting, Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> INDEX_ROUTING_REQUIRE_SETTING = Setting.simpleString(INDEX_ROUTING_REQUIRE,
        DataTierAllocationDecider::validateTierSetting, Setting.Property.Dynamic, Setting.Property.IndexScope);
    public static final Setting<String> INDEX_ROUTING_INCLUDE_SETTING = Setting.simpleString(INDEX_ROUTING_INCLUDE,
        DataTierAllocationDecider::validateTierSetting, Setting.Property.Dynamic, Setting.Property.IndexScope);
    public static final Setting<String> INDEX_ROUTING_EXCLUDE_SETTING = Setting.simpleString(INDEX_ROUTING_EXCLUDE,
        DataTierAllocationDecider::validateTierSetting, Setting.Property.Dynamic, Setting.Property.IndexScope);

    private static void validateTierSetting(String setting) {
        if (Strings.hasText(setting)) {
            Set<String> invalidTiers = Arrays.stream(setting.split(","))
                .filter(tier -> DataTier.validTierName(tier) == false)
                .collect(Collectors.toSet());
            if (invalidTiers.size() > 0) {
                throw new IllegalArgumentException("invalid tier names: " + invalidTiers);
            }
        }
    }

    private volatile String clusterRequire = null;
    private volatile String clusterInclude = null;
    private volatile String clusterExclude = null;

    public DataTierAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_REQUIRE_SETTING, s -> this.clusterRequire = s);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_INCLUDE_SETTING, s -> this.clusterInclude = s);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_EXCLUDE_SETTING, s -> this.clusterExclude = s);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(shardRouting, node.node(), allocation);
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(indexMetadata, node.node(), allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(shardRouting, node.node(), allocation);
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        Decision decision = shouldClusterFilter(node, allocation);
        if (decision != null) {
            return decision;
        }

        decision = shouldIndexFilter(indexMetadata, node, allocation);
        if (decision != null) {
            return decision;
        }

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require tier filters");
    }

    private Decision shouldFilter(ShardRouting shardRouting, DiscoveryNode node, RoutingAllocation allocation) {
        Decision decision = shouldClusterFilter(node, allocation);
        if (decision != null) {
            return decision;
        }

        decision = shouldIndexFilter(allocation.metadata().getIndexSafe(shardRouting.index()), node, allocation);
        if (decision != null) {
            return decision;
        }

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require tier filters");
    }

    private Decision shouldFilter(IndexMetadata indexMd, DiscoveryNode node, RoutingAllocation allocation) {
        Decision decision = shouldClusterFilter(node, allocation);
        if (decision != null) {
            return decision;
        }

        decision = shouldIndexFilter(indexMd, node, allocation);
        if (decision != null) {
            return decision;
        }

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require tier filters");
    }

    private Decision shouldIndexFilter(IndexMetadata indexMd, DiscoveryNode node, RoutingAllocation allocation) {
        Settings indexSettings = indexMd.getSettings();
        String indexRequire = INDEX_ROUTING_REQUIRE_SETTING.get(indexSettings);
        String indexInclude = INDEX_ROUTING_INCLUDE_SETTING.get(indexSettings);
        String indexExclude = INDEX_ROUTING_EXCLUDE_SETTING.get(indexSettings);

        if (Strings.hasText(indexRequire)) {
            if (allocationAllowed(OpType.AND, indexRequire, node) == false) {
                return allocation.decision(Decision.NO, NAME, "node does not match all index setting [%s] tier filters [%s]",
                    INDEX_ROUTING_REQUIRE, indexRequire);
            }
        }
        if (Strings.hasText(indexInclude)) {
            if (allocationAllowed(OpType.OR, indexInclude, node) == false) {
                return allocation.decision(Decision.NO, NAME, "node does not match any index setting [%s] tier filters [%s]",
                    INDEX_ROUTING_INCLUDE, indexInclude);
            }
        }
        if (Strings.hasText(indexExclude)) {
            if (allocationAllowed(OpType.OR, indexExclude, node)) {
                return allocation.decision(Decision.NO, NAME, "node matches any index setting [%s] tier filters [%s]",
                    INDEX_ROUTING_EXCLUDE, indexExclude);
            }
        }
        return null;
    }

    private Decision shouldClusterFilter(DiscoveryNode node, RoutingAllocation allocation) {
        if (Strings.hasText(clusterRequire)) {
            if (allocationAllowed(OpType.AND, clusterRequire, node) == false) {
                return allocation.decision(Decision.NO, NAME, "node does not match all cluster setting [%s] tier filters [%s]",
                    CLUSTER_ROUTING_REQUIRE, clusterRequire);
            }
        }
        if (Strings.hasText(clusterInclude)) {
            if (allocationAllowed(OpType.OR, clusterInclude, node) == false) {
                return allocation.decision(Decision.NO, NAME, "node does not match any cluster setting [%s] tier filters [%s]",
                    CLUSTER_ROUTING_INCLUDE, clusterInclude);
            }
        }
        if (Strings.hasText(clusterExclude)) {
            if (allocationAllowed(OpType.OR, clusterExclude, node)) {
                return allocation.decision(Decision.NO, NAME, "node matches any cluster setting [%s] tier filters [%s]",
                    CLUSTER_ROUTING_EXCLUDE, clusterExclude);
            }
        }
        return null;
    }

    private enum OpType {
        AND,
        OR
    }

    private static boolean allocationAllowed(OpType opType, String tierSetting, DiscoveryNode node) {
        String[] values = Strings.tokenizeToStringArray(tierSetting, ",");
        for (String value : values) {
            // generic "data" roles are considered to have all tiers
            if (node.getRoles().contains(DiscoveryNodeRole.DATA_ROLE) ||
                node.getRoles().stream().map(DiscoveryNodeRole::roleName).collect(Collectors.toSet()).contains(value)) {
                if (opType == OpType.OR) {
                    return true;
                }
            } else {
                if (opType == OpType.AND) {
                    return false;
                }
            }
        }
        if (opType == OpType.OR) {
            return false;
        } else {
            return true;
        }
    }
}
