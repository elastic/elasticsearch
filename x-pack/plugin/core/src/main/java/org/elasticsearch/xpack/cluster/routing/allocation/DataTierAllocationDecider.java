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

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The {@code DataTierAllocationDecider} is a custom allocation decider that behaves similar to the
 * {@link org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider}, however it
 * is specific to the {@code _tier} setting for both the cluster and index level.
 */
public final class DataTierAllocationDecider extends AllocationDecider {

    public static final String NAME = "data_tier";

    public static final DataTierAllocationDecider INSTANCE = new DataTierAllocationDecider();

    private DataTierAllocationDecider() {}

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

    private static Decision shouldFilter(IndexMetadata indexMd, Set<DiscoveryNodeRole> roles, RoutingAllocation allocation) {
        return shouldFilter(indexMd, roles, DataTierAllocationDecider::preferredAvailableTier, allocation);
    }

    public interface PreferredTierFunction {
        Optional<String> apply(List<String> tierPreference, DiscoveryNodes nodes);
    }

    private static final Decision YES_PASSES = Decision.single(Decision.YES.type(), NAME, "node passes tier preference filters");

    public static Decision shouldFilter(
        IndexMetadata indexMd,
        Set<DiscoveryNodeRole> roles,
        PreferredTierFunction preferredTierFunction,
        RoutingAllocation allocation
    ) {
        List<String> tierPreference = indexMd.getTierPreference();
        if (tierPreference.isEmpty() != false) {
            return YES_PASSES;
        }
        Optional<String> tier = preferredTierFunction.apply(tierPreference, allocation.nodes());
        if (tier.isPresent()) {
            String tierName = tier.get();
            if (allocationAllowed(tierName, roles)) {
                if (allocation.debugDecision()) {
                    return debugYesAllowed(allocation, tierPreference, tierName);
                }
                return Decision.YES;
            }
            if (allocation.debugDecision()) {
                return debugNoRequirementsNotMet(allocation, tierPreference, tierName);
            }
            return Decision.NO;
        }
        if (allocation.debugDecision()) {
            return debugNoNoNodesAvailable(allocation, tierPreference);
        }
        return Decision.NO;
    }

    private static Decision debugNoNoNodesAvailable(RoutingAllocation allocation, List<String> tierPreference) {
        return allocation.decision(
            Decision.NO,
            NAME,
            "index has a preference for tiers [%s], but no nodes for any of those tiers are available in the cluster",
            String.join(",", tierPreference)
        );
    }

    private static Decision debugNoRequirementsNotMet(RoutingAllocation allocation, List<String> tierPreference, String tierName) {
        return allocation.decision(
            Decision.NO,
            NAME,
            "index has a preference for tiers [%s] and node does not meet the required [%s] tier",
            String.join(",", tierPreference),
            tierName
        );
    }

    private static Decision debugYesAllowed(RoutingAllocation allocation, List<String> tierPreference, String tierName) {
        return allocation.decision(
            Decision.YES,
            NAME,
            "index has a preference for tiers [%s] and node has tier [%s]",
            String.join(",", tierPreference),
            tierName
        );
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
        for (DiscoveryNode node : nodes) {
            if (allocationAllowed(singleTier, node.getRoles())) {
                return true;
            }
        }
        return false;
    }

    private static boolean allocationAllowed(String tierName, Set<DiscoveryNodeRole> roles) {
        assert Strings.hasText(tierName) : "tierName must be not null and non-empty, but was [" + tierName + "]";

        if (roles.contains(DiscoveryNodeRole.DATA_ROLE)) {
            // generic "data" roles are considered to have all tiers
            return true;
        }
        for (DiscoveryNodeRole role : roles) {
            if (tierName.equals(role.roleName())) {
                return true;
            }
        }
        return false;
    }
}
