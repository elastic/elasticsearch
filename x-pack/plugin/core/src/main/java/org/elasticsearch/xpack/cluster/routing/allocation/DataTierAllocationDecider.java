/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Optional;
import java.util.Set;

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
    public static final String TIER_PREFERENCE = "index.routing.allocation.include._tier_preference";

    public static final Setting<String> CLUSTER_ROUTING_REQUIRE_SETTING = Setting.simpleString(
        CLUSTER_ROUTING_REQUIRE,
        DataTierAllocationDecider::validateTierSetting,
        Property.Dynamic,
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<String> CLUSTER_ROUTING_INCLUDE_SETTING = Setting.simpleString(
        CLUSTER_ROUTING_INCLUDE,
        DataTierAllocationDecider::validateTierSetting,
        Property.Dynamic,
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<String> CLUSTER_ROUTING_EXCLUDE_SETTING = Setting.simpleString(
        CLUSTER_ROUTING_EXCLUDE,
        DataTierAllocationDecider::validateTierSetting,
        Property.Dynamic,
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<String> INDEX_ROUTING_REQUIRE_SETTING = Setting.simpleString(
        INDEX_ROUTING_REQUIRE,
        DataTier.DATA_TIER_SETTING_VALIDATOR,
        Property.Dynamic,
        Property.IndexScope,
        Property.Deprecated
    );
    public static final Setting<String> INDEX_ROUTING_INCLUDE_SETTING = Setting.simpleString(
        INDEX_ROUTING_INCLUDE,
        DataTier.DATA_TIER_SETTING_VALIDATOR,
        Property.Dynamic,
        Property.IndexScope,
        Property.Deprecated
    );
    public static final Setting<String> INDEX_ROUTING_EXCLUDE_SETTING = Setting.simpleString(
        INDEX_ROUTING_EXCLUDE,
        DataTier.DATA_TIER_SETTING_VALIDATOR,
        Property.Dynamic,
        Property.IndexScope,
        Property.Deprecated
    );

    private static void validateTierSetting(String setting) {
        if (Strings.hasText(setting)) {
            for (String s : setting.split(",")) {
                if (DataTier.validTierName(s) == false) {
                    throw new IllegalArgumentException(
                        "invalid tier names found in [" + setting + "] allowed values are " + DataTier.ALL_DATA_TIERS
                    );
                }
            }
        }
    }

    private volatile String clusterRequire;
    private volatile String clusterInclude;
    private volatile String clusterExclude;

    public DataTierAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        clusterRequire = CLUSTER_ROUTING_REQUIRE_SETTING.get(settings);
        clusterInclude = CLUSTER_ROUTING_INCLUDE_SETTING.get(settings);
        clusterExclude = CLUSTER_ROUTING_EXCLUDE_SETTING.get(settings);
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
        return shouldFilter(indexMetadata, node.node().getRoles(), allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(shardRouting, node.node(), allocation);
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return shouldFilter(indexMetadata, node.getRoles(), allocation);
    }

    private Decision shouldFilter(ShardRouting shardRouting, DiscoveryNode node, RoutingAllocation allocation) {
        return shouldFilter(allocation.metadata().getIndexSafe(shardRouting.index()), node.getRoles(), allocation);
    }

    public Decision shouldFilter(IndexMetadata indexMd, Set<DiscoveryNodeRole> roles, RoutingAllocation allocation) {
        return shouldFilter(indexMd, roles, DataTierAllocationDecider::preferredAvailableTier, allocation);
    }

    public interface PreferredTierFunction {
        Optional<String> apply(List<String> tierPreference, DiscoveryNodes nodes);
    }

    public Decision shouldFilter(
        IndexMetadata indexMd,
        Set<DiscoveryNodeRole> roles,
        PreferredTierFunction preferredTierFunction,
        RoutingAllocation allocation
    ) {
        Decision decision = shouldClusterFilter(roles, allocation);
        if (decision != null) {
            return decision;
        }

        decision = shouldIndexFilter(indexMd, roles, allocation);
        if (decision != null) {
            return decision;
        }

        decision = shouldIndexPreferTier(indexMd, roles, preferredTierFunction, allocation);
        if (decision != null) {
            return decision;
        }

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require/prefer tier filters");
    }

    private Decision shouldIndexPreferTier(
        IndexMetadata indexMetadata,
        Set<DiscoveryNodeRole> roles,
        PreferredTierFunction preferredTierFunction,
        RoutingAllocation allocation
    ) {
        List<String> tierPreference = indexMetadata.getTierPreference();

        if (tierPreference.isEmpty() == false) {
            Optional<String> tier = preferredTierFunction.apply(tierPreference, allocation.nodes());
            if (tier.isPresent()) {
                String tierName = tier.get();
                if (allocationAllowed(tierName, roles)) {
                    if (allocation.debugDecision() == false) {
                        return Decision.YES;
                    }
                    return allocation.decision(
                        Decision.YES,
                        NAME,
                        "index has a preference for tiers [%s] and node has tier [%s]",
                        String.join(",", tierPreference),
                        tierName
                    );
                } else {
                    if (allocation.debugDecision() == false) {
                        return Decision.NO;
                    }
                    return allocation.decision(
                        Decision.NO,
                        NAME,
                        "index has a preference for tiers [%s] and node does not meet the required [%s] tier",
                        String.join(",", tierPreference),
                        tierName
                    );
                }
            } else {
                if (allocation.debugDecision() == false) {
                    return Decision.NO;
                }
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "index has a preference for tiers [%s], " + "but no nodes for any of those tiers are available in the cluster",
                    String.join(",", tierPreference)
                );
            }
        }
        return null;
    }

    private Decision shouldIndexFilter(IndexMetadata indexMd, Set<DiscoveryNodeRole> roles, RoutingAllocation allocation) {
        Settings indexSettings = indexMd.getSettings();
        String indexRequire = INDEX_ROUTING_REQUIRE_SETTING.get(indexSettings);
        String indexInclude = INDEX_ROUTING_INCLUDE_SETTING.get(indexSettings);
        String indexExclude = INDEX_ROUTING_EXCLUDE_SETTING.get(indexSettings);

        if (Strings.hasText(indexRequire)) {
            if (allocationAllowed(OpType.AND, indexRequire, roles) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match all index setting [%s] tier filters [%s]",
                    INDEX_ROUTING_REQUIRE,
                    indexRequire
                );
            }
        }
        if (Strings.hasText(indexInclude)) {
            if (allocationAllowed(OpType.OR, indexInclude, roles) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match any index setting [%s] tier filters [%s]",
                    INDEX_ROUTING_INCLUDE,
                    indexInclude
                );
            }
        }
        if (Strings.hasText(indexExclude)) {
            if (allocationAllowed(OpType.OR, indexExclude, roles)) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node matches any index setting [%s] tier filters [%s]",
                    INDEX_ROUTING_EXCLUDE,
                    indexExclude
                );
            }
        }
        return null;
    }

    private Decision shouldClusterFilter(Set<DiscoveryNodeRole> roles, RoutingAllocation allocation) {
        if (Strings.hasText(clusterRequire)) {
            if (allocationAllowed(OpType.AND, clusterRequire, roles) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match all cluster setting [%s] tier filters [%s]",
                    CLUSTER_ROUTING_REQUIRE,
                    clusterRequire
                );
            }
        }
        if (Strings.hasText(clusterInclude)) {
            if (allocationAllowed(OpType.OR, clusterInclude, roles) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match any cluster setting [%s] tier filters [%s]",
                    CLUSTER_ROUTING_INCLUDE,
                    clusterInclude
                );
            }
        }
        if (Strings.hasText(clusterExclude)) {
            if (allocationAllowed(OpType.OR, clusterExclude, roles)) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node matches any cluster setting [%s] tier filters [%s]",
                    CLUSTER_ROUTING_EXCLUDE,
                    clusterExclude
                );
            }
        }
        return null;
    }

    private enum OpType {
        AND,
        OR
    }

    /**
     * Given a string of comma-separated prioritized tiers (highest priority
     * first) and an allocation, find the highest priority tier for which nodes
     * exist. If no nodes for any of the tiers are available, returns an empty
     * {@code Optional<String>}.
     */
    public static Optional<String> preferredAvailableTier(List<String> prioritizedTiers, DiscoveryNodes nodes) {
        for (String tier : prioritizedTiers) {
            if (tierNodesPresent(tier, nodes)) {
                return Optional.of(tier);
            }
        }
        return Optional.empty();
    }

    static boolean tierNodesPresent(String singleTier, DiscoveryNodes nodes) {
        assert singleTier.equals(DiscoveryNodeRole.DATA_ROLE.roleName()) || DataTier.validTierName(singleTier)
            : "tier " + singleTier + " is an invalid tier name";
        for (DiscoveryNode node : nodes.getNodes().values()) {
            for (DiscoveryNodeRole discoveryNodeRole : node.getRoles()) {
                String s = discoveryNodeRole.roleName();
                if (s.equals(DiscoveryNodeRole.DATA_ROLE.roleName()) || s.equals(singleTier)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean allocationAllowed(OpType opType, String tierSetting, Set<DiscoveryNodeRole> roles) {
        assert Strings.hasText(tierSetting) : "tierName must be not null and non-empty, but was [" + tierSetting + "]";

        if (roles.contains(DiscoveryNodeRole.DATA_ROLE)) {
            // generic "data" roles are considered to have all tiers
            return true;
        }
        List<String> values = DataTier.parseTierList(tierSetting);
        for (String tierName : values) {
            boolean containsName = false;
            for (DiscoveryNodeRole role : roles) {
                if (tierName.equals(role.roleName())) {
                    containsName = true;
                    break;
                }
            }
            if (containsName) {
                if (opType == OpType.OR) {
                    return true;
                }
            } else if (opType == OpType.AND) {
                return false;
            }
        }
        return opType == OpType.AND;
    }

    private static boolean allocationAllowed(String tierName, Set<DiscoveryNodeRole> roles) {
        assert Strings.hasText(tierName) : "tierName must be not null and non-empty, but was [" + tierName + "]";

        if (roles.contains(DiscoveryNodeRole.DATA_ROLE)) {
            // generic "data" roles are considered to have all tiers
            return true;
        } else {
            for (DiscoveryNodeRole role : roles) {
                if (tierName.equals(role.roleName())) {
                    return true;
                }
            }
            return false;
        }
    }
}
