/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.List;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.DataTier.DATA_FROZEN;

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
    public static final String INDEX_ROUTING_PREFER = "index.routing.allocation.include._tier_preference";
    public static final String INDEX_ROUTING_EXCLUDE = "index.routing.allocation.exclude._tier";

    private static final DataTierValidator VALIDATOR = new DataTierValidator();
    public static final Setting<String> CLUSTER_ROUTING_REQUIRE_SETTING = Setting.simpleString(CLUSTER_ROUTING_REQUIRE,
        DataTierAllocationDecider::validateTierSetting, Setting.Property.Dynamic, Setting.Property.NodeScope, Setting.Property.Deprecated);
    public static final Setting<String> CLUSTER_ROUTING_INCLUDE_SETTING = Setting.simpleString(CLUSTER_ROUTING_INCLUDE,
        DataTierAllocationDecider::validateTierSetting, Setting.Property.Dynamic, Setting.Property.NodeScope, Setting.Property.Deprecated);
    public static final Setting<String> CLUSTER_ROUTING_EXCLUDE_SETTING = Setting.simpleString(CLUSTER_ROUTING_EXCLUDE,
        DataTierAllocationDecider::validateTierSetting, Setting.Property.Dynamic, Setting.Property.NodeScope, Setting.Property.Deprecated);
    public static final Setting<String> INDEX_ROUTING_REQUIRE_SETTING = Setting.simpleString(INDEX_ROUTING_REQUIRE,
        VALIDATOR, Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.Deprecated);
    public static final Setting<String> INDEX_ROUTING_INCLUDE_SETTING = Setting.simpleString(INDEX_ROUTING_INCLUDE,
        VALIDATOR, Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.Deprecated);
    public static final Setting<String> INDEX_ROUTING_EXCLUDE_SETTING = Setting.simpleString(INDEX_ROUTING_EXCLUDE,
        VALIDATOR, Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.Deprecated);
    public static final Setting<String> INDEX_ROUTING_PREFER_SETTING = new Setting<String>(new Setting.SimpleKey(INDEX_ROUTING_PREFER),
        DataTierValidator::getDefaultTierPreference, Function.identity(), VALIDATOR,
        Setting.Property.Dynamic, Setting.Property.IndexScope) {
        @Override
        public String get(Settings settings) {
            if (SearchableSnapshotsSettings.isPartialSearchableSnapshotIndex(settings)) {
                // Partial searchable snapshot indices should be restricted to
                // only data_frozen when reading the setting, or else validation fails.
                return DATA_FROZEN;
            } else {
                // Otherwise pass through to the regular setting retrieval
                return super.get(settings);
            }
        }
    };

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

    private static class DataTierValidator implements Setting.Validator<String> {
        private static final Collection<Setting<?>> dependencies =
            List.of(IndexModule.INDEX_STORE_TYPE_SETTING,
                SearchableSnapshotsConstants.SNAPSHOT_PARTIAL_SETTING);

        public static String getDefaultTierPreference(Settings settings) {
            if (SearchableSnapshotsSettings.isPartialSearchableSnapshotIndex(settings)) {
                return DATA_FROZEN;
            } else {
                return "";
            }
        }

        @Override
        public void validate(String value) {
            validateTierSetting(value);
        }

        @Override
        public void validate(String value, Map<Setting<?>, Object> settings, boolean exists) {
            if (exists && value != null) {
                if (SearchableSnapshotsConstants.isPartialSearchableSnapshotIndex(settings)) {
                    if (value.equals(DATA_FROZEN) == false) {
                        throw new IllegalArgumentException("only the [" + DATA_FROZEN +
                            "] tier preference may be used for partial searchable snapshots (got: [" + value + "])");
                    }
                } else {
                    String[] split = value.split(",");
                    if (Arrays.stream(split).anyMatch(DATA_FROZEN::equals)) {
                        throw new IllegalArgumentException("[" + DATA_FROZEN + "] tier can only be used for partial searchable snapshots");
                    }
                }
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return dependencies.iterator();
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
        Optional<String> apply(String tierPreference, DiscoveryNodes nodes);
    }

    public Decision shouldFilter(IndexMetadata indexMd, Set<DiscoveryNodeRole> roles,
                                 PreferredTierFunction preferredTierFunction, RoutingAllocation allocation) {
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

    private Decision shouldIndexPreferTier(IndexMetadata indexMetadata, Set<DiscoveryNodeRole> roles,
                                           PreferredTierFunction preferredTierFunction, RoutingAllocation allocation) {
        Settings indexSettings = indexMetadata.getSettings();
        String tierPreference = INDEX_ROUTING_PREFER_SETTING.get(indexSettings);

        if (Strings.hasText(tierPreference)) {
            Optional<String> tier = preferredTierFunction.apply(tierPreference, allocation.nodes());
            if (tier.isPresent()) {
                String tierName = tier.get();
                // The OpType doesn't actually matter here, because we have
                // selected only a single tier as our "preferred" tier
                if (allocationAllowed(OpType.AND, tierName, roles)) {
                    return allocation.decision(Decision.YES, NAME,
                        "index has a preference for tiers [%s] and node has tier [%s]", tierPreference, tierName);
                } else {
                    return allocation.decision(Decision.NO, NAME,
                        "index has a preference for tiers [%s] and node does not meet the required [%s] tier", tierPreference, tierName);
                }
            } else {
                return allocation.decision(Decision.NO, NAME, "index has a preference for tiers [%s], " +
                    "but no nodes for any of those tiers are available in the cluster", tierPreference);
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
                return allocation.decision(Decision.NO, NAME, "node does not match all index setting [%s] tier filters [%s]",
                    INDEX_ROUTING_REQUIRE, indexRequire);
            }
        }
        if (Strings.hasText(indexInclude)) {
            if (allocationAllowed(OpType.OR, indexInclude, roles) == false) {
                return allocation.decision(Decision.NO, NAME, "node does not match any index setting [%s] tier filters [%s]",
                    INDEX_ROUTING_INCLUDE, indexInclude);
            }
        }
        if (Strings.hasText(indexExclude)) {
            if (allocationAllowed(OpType.OR, indexExclude, roles)) {
                return allocation.decision(Decision.NO, NAME, "node matches any index setting [%s] tier filters [%s]",
                    INDEX_ROUTING_EXCLUDE, indexExclude);
            }
        }
        return null;
    }

    private Decision shouldClusterFilter(Set<DiscoveryNodeRole> roles, RoutingAllocation allocation) {
        if (Strings.hasText(clusterRequire)) {
            if (allocationAllowed(OpType.AND, clusterRequire, roles) == false) {
                return allocation.decision(Decision.NO, NAME, "node does not match all cluster setting [%s] tier filters [%s]",
                    CLUSTER_ROUTING_REQUIRE, clusterRequire);
            }
        }
        if (Strings.hasText(clusterInclude)) {
            if (allocationAllowed(OpType.OR, clusterInclude, roles) == false) {
                return allocation.decision(Decision.NO, NAME, "node does not match any cluster setting [%s] tier filters [%s]",
                    CLUSTER_ROUTING_INCLUDE, clusterInclude);
            }
        }
        if (Strings.hasText(clusterExclude)) {
            if (allocationAllowed(OpType.OR, clusterExclude, roles)) {
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

    /**
     * Given a string of comma-separated prioritized tiers (highest priority
     * first) and an allocation, find the highest priority tier for which nodes
     * exist. If no nodes for any of the tiers are available, returns an empty
     * {@code Optional<String>}.
     */
    public static Optional<String> preferredAvailableTier(String prioritizedTiers, DiscoveryNodes nodes) {
        String[] tiers = parseTierList(prioritizedTiers);
        return Arrays.stream(tiers).filter(tier -> tierNodesPresent(tier, nodes)).findFirst();
    }

    public static String[] parseTierList(String tiers) {
        return Strings.tokenizeToStringArray(tiers, ",");
    }

    static boolean tierNodesPresent(String singleTier, DiscoveryNodes nodes) {
        assert singleTier.equals(DiscoveryNodeRole.DATA_ROLE.roleName()) || DataTier.validTierName(singleTier) :
            "tier " + singleTier + " is an invalid tier name";
        for (ObjectCursor<DiscoveryNode> node : nodes.getNodes().values()) {
            if (node.value.getRoles().stream()
                .map(DiscoveryNodeRole::roleName)
                .anyMatch(s -> s.equals(DiscoveryNodeRole.DATA_ROLE.roleName()) || s.equals(singleTier))) {
                return true;
            }
        }
        return false;
    }


    private static boolean allocationAllowed(OpType opType, String tierSetting, Set<DiscoveryNodeRole> roles) {
        String[] values = parseTierList(tierSetting);
        for (String value : values) {
            // generic "data" roles are considered to have all tiers
            if (roles.contains(DiscoveryNodeRole.DATA_ROLE) ||
                roles.stream().map(DiscoveryNodeRole::roleName).collect(Collectors.toSet()).contains(value)) {
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
