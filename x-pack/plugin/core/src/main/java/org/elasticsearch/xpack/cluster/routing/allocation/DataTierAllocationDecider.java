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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.DataTier.DATA_FROZEN;

/**
 * The {@code DataTierAllocationDecider} is a custom allocation decider that behaves similar to the
 * {@link org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider}, however it
 * is specific to the {@code _tier} setting for both the cluster and index level.
 */
public class DataTierAllocationDecider extends AllocationDecider {

    public static final String NAME = "data_tier";

    public static final String TIER_PREFERENCE = "index.routing.allocation.include._tier_preference";

    private static final DataTierValidator VALIDATOR = new DataTierValidator();
    public static final Setting<String> TIER_PREFERENCE_SETTING = new Setting<>(new Setting.SimpleKey(TIER_PREFERENCE),
        DataTierValidator::getDefaultTierPreference, Function.identity(), VALIDATOR, Property.Dynamic, Property.IndexScope);

    private static void validateTierSetting(String setting) {
        if (Strings.hasText(setting)) {
            for (String s : setting.split(",")) {
                if (DataTier.validTierName(s) == false) {
                    throw new IllegalArgumentException(
                        "invalid tier names found in [" + setting + "] allowed values are " + DataTier.ALL_DATA_TIERS);
                }
            }
        }
    }

    private static class DataTierValidator implements Setting.Validator<String> {

        private static final Collection<Setting<?>> dependencies = List.of(
            IndexModule.INDEX_STORE_TYPE_SETTING,
            SearchableSnapshotsConstants.SNAPSHOT_PARTIAL_SETTING
        );

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
                    if (value.contains(DATA_FROZEN)) {
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

    public DataTierAllocationDecider() {
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
        Decision decision = shouldIndexPreferTier(indexMd, roles, preferredTierFunction, allocation);
        if (decision != null) {
            return decision;
        }

        return allocation.decision(Decision.YES, NAME, "node passes tier preference filters");
    }

    private Decision shouldIndexPreferTier(IndexMetadata indexMetadata, Set<DiscoveryNodeRole> roles,
                                           PreferredTierFunction preferredTierFunction, RoutingAllocation allocation) {
        Settings indexSettings = indexMetadata.getSettings();
        String tierPreference = TIER_PREFERENCE_SETTING.get(indexSettings);

        if (Strings.hasText(tierPreference)) {
            Optional<String> tier = preferredTierFunction.apply(tierPreference, allocation.nodes());
            if (tier.isPresent()) {
                String tierName = tier.get();
                if (allocationAllowed(tierName, roles)) {
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

    /**
     * Given a string of comma-separated prioritized tiers (highest priority
     * first) and an allocation, find the highest priority tier for which nodes
     * exist. If no nodes for any of the tiers are available, returns an empty
     * {@code Optional<String>}.
     */
    public static Optional<String> preferredAvailableTier(String prioritizedTiers, DiscoveryNodes nodes) {
        for (String tier : parseTierList(prioritizedTiers)) {
            if (tierNodesPresent(tier, nodes)) {
                return Optional.of(tier);
            }
        }
        return Optional.empty();
    }

    public static String[] parseTierList(String tiers) {
        if (Strings.hasText(tiers) == false) {
            // avoid parsing overhead in the null/empty string case
            return Strings.EMPTY_ARRAY;
        } else {
            return tiers.split(",");
        }
    }

    static boolean tierNodesPresent(String singleTier, DiscoveryNodes nodes) {
        assert singleTier.equals(DiscoveryNodeRole.DATA_ROLE.roleName()) || DataTier.validTierName(singleTier) :
            "tier " + singleTier + " is an invalid tier name";
        for (ObjectCursor<DiscoveryNode> node : nodes.getNodes().values()) {
            for (DiscoveryNodeRole discoveryNodeRole : node.value.getRoles()) {
                String s = discoveryNodeRole.roleName();
                if (s.equals(DiscoveryNodeRole.DATA_ROLE.roleName()) || s.equals(singleTier)) {
                    return true;
                }
            }
        }
        return false;
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
