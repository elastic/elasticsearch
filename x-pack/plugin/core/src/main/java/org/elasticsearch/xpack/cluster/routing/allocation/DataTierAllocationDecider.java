/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
        return shouldFilter(allocation.metadata().indexMetadata(shardRouting.index()), node.node(), allocation);
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(indexMetadata, node.node(), allocation);
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(indexMetadata, node.node(), allocation);
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return shouldFilter(indexMetadata, node, allocation);
    }

    private static Decision shouldFilter(IndexMetadata indexMd, DiscoveryNode node, RoutingAllocation allocation) {
        return shouldFilter(indexMd, node, DataTierAllocationDecider::preferredAvailableTier, allocation);
    }

    public interface PreferredTierFunction {
        Optional<String> apply(
            List<String> tierPreference,
            DiscoveryNodes nodes,
            DesiredNodes desiredNodes,
            NodesShutdownMetadata shutdownMetadata
        );
    }

    private static final Decision YES_PASSES = Decision.single(Decision.YES.type(), NAME, "node passes tier preference filters");

    public static Decision shouldFilter(
        IndexMetadata indexMd,
        DiscoveryNode node,
        PreferredTierFunction preferredTierFunction,
        RoutingAllocation allocation
    ) {
        List<String> tierPreference = indexMd.getTierPreference();
        if (tierPreference.isEmpty()) {
            return YES_PASSES;
        }
        Optional<String> tier = preferredTierFunction.apply(
            tierPreference,
            allocation.nodes(),
            allocation.desiredNodes(),
            allocation.getClusterState().metadata().nodeShutdowns()
        );
        if (tier.isPresent()) {
            String tierName = tier.get();
            assert Strings.hasText(tierName) : "tierName must be not null and non-empty, but was [" + tierName + "]";
            if (node.hasRole(DiscoveryNodeRole.DATA_ROLE.roleName())) {
                return allocation.debugDecision()
                    ? debugYesAllowed(allocation, tierPreference, DiscoveryNodeRole.DATA_ROLE.roleName())
                    : Decision.YES;
            }
            if (node.hasRole(tierName)) {
                return allocation.debugDecision() ? debugYesAllowed(allocation, tierPreference, tierName) : Decision.YES;
            }
            return allocation.debugDecision() ? debugNoRequirementsNotMet(allocation, tierPreference, tierName) : Decision.NO;
        }
        return allocation.debugDecision() ? debugNoNoNodesAvailable(allocation, tierPreference) : Decision.NO;
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
     * {@code Optional<String>}. This method takes into account the desired nodes
     * in order to know if there are planned topology changes in the cluster
     * that can remove a tier that's part of the cluster now.
     */
    public static Optional<String> preferredAvailableTier(
        List<String> prioritizedTiers,
        DiscoveryNodes nodes,
        DesiredNodes desiredNodes,
        NodesShutdownMetadata shutdownMetadata
    ) {

        final var desiredNodesPreferredTier = getPreferredTierFromDesiredNodes(prioritizedTiers, nodes, desiredNodes);

        if (desiredNodesPreferredTier.isPresent()) {
            return desiredNodesPreferredTier;
        }

        return getPreferredAvailableTierFromClusterMembers(prioritizedTiers, nodes, removingNodeIds(shutdownMetadata));
    }

    /**
     * Given a list of tiers in descending order, return the tier that's present
     * in the desired nodes with the highest priority, if none is present returns an
     * {@code Optional.empty()}.
     */
    public static Optional<String> getPreferredTierFromDesiredNodes(
        List<String> prioritizedTiers,
        DiscoveryNodes discoveryNodes,
        DesiredNodes desiredNodes
    ) {
        if (desiredNodes == null) {
            return Optional.empty();
        }

        for (int tierIndex = 0; tierIndex < prioritizedTiers.size(); tierIndex++) {
            final var tier = prioritizedTiers.get(tierIndex);
            if (tierNodesPresent(tier, desiredNodes.actualized())
                || isDesiredNodeWithinTierJoining(tier, discoveryNodes, desiredNodes)
                || nextTierIsGrowingAndCurrentTierCanHoldTheIndex(prioritizedTiers, tierIndex, discoveryNodes, desiredNodes)) {
                return Optional.of(tier);
            }
        }
        return Optional.empty();
    }

    private static boolean nextTierIsGrowingAndCurrentTierCanHoldTheIndex(
        List<String> prioritizedTiers,
        int tierIndex,
        DiscoveryNodes discoveryNodes,
        DesiredNodes desiredNodes
    ) {
        final var tier = prioritizedTiers.get(tierIndex);
        assert tierNodesPresent(tier, desiredNodes.actualized()) == false;
        // If there's a plan to grow the next preferred tier, and it hasn't materialized yet,
        // wait until all the nodes in the next tier have joined. This would avoid overwhelming
        // the next tier if within the same plan one tier is removed and the next preferred tier
        // grows.
        boolean nextPreferredTierIsGrowing = false;
        for (int i = tierIndex + 1; i < prioritizedTiers.size(); i++) {
            final var nextTier = prioritizedTiers.get(i);
            nextPreferredTierIsGrowing |= tierNodesPresent(nextTier, desiredNodes.pending());
        }
        return tierNodesPresent(tier, discoveryNodes) && nextPreferredTierIsGrowing;
    }

    private static boolean isDesiredNodeWithinTierJoining(String tier, DiscoveryNodes discoveryNodes, DesiredNodes desiredNodes) {
        assert tierNodesPresent(tier, desiredNodes.actualized()) == false;
        // Take into account the case when the desired nodes have been updated and the node in the tier would be replaced by
        // a new one. In that case the desired node in the tier won't be actualized as it has to join, but we still need to ensure
        // that at least one cluster member has the requested tier as we would prefer to minimize the shard movements in these cases.
        return tierNodesPresent(tier, desiredNodes.pending()) && tierNodesPresent(tier, discoveryNodes);
    }

    private static Optional<String> getPreferredAvailableTierFromClusterMembers(
        List<String> prioritizedTiers,
        DiscoveryNodes nodes,
        Set<String> removingNodeIds
    ) {
        for (String tier : prioritizedTiers) {
            if (tierNodesPresentConsideringRemovals(tier, nodes, removingNodeIds)) {
                return Optional.of(tier);
            }
        }
        return Optional.empty();
    }

    static boolean tierNodesPresent(String singleTier, Collection<DesiredNode> nodes) {
        assert singleTier.equals(DiscoveryNodeRole.DATA_ROLE.roleName()) || DataTier.validTierName(singleTier)
            : "tier " + singleTier + " is an invalid tier name";
        for (DesiredNode node : nodes) {
            if (allocationAllowed(singleTier, node.getRoles())) {
                return true;
            }
        }
        return false;
    }

    // This overload for Desired Nodes codepaths, which do not consider Node Shutdown, as Desired Nodes takes precedence
    static boolean tierNodesPresent(String singleTier, DiscoveryNodes nodes) {
        return tierNodesPresentConsideringRemovals(singleTier, nodes, Collections.emptySet());
    }

    static boolean tierNodesPresentConsideringRemovals(String singleTier, DiscoveryNodes nodes, Set<String> removingNodeIds) {
        assert singleTier.equals(DiscoveryNodeRole.DATA_ROLE.roleName()) || DataTier.validTierName(singleTier)
            : "tier " + singleTier + " is an invalid tier name";
        var rolesToNodes = nodes.getTiersToNodeIds();
        Set<String> nodesWithTier = rolesToNodes.getOrDefault(singleTier, Collections.emptySet());
        Set<String> dataNodes = rolesToNodes.getOrDefault(DiscoveryNodeRole.DATA_ROLE.roleName(), Collections.emptySet());

        if (removingNodeIds.isEmpty()) {
            return nodesWithTier.isEmpty() == false || dataNodes.isEmpty() == false;
        } else if (removingNodeIds.size() < nodesWithTier.size() || removingNodeIds.size() < dataNodes.size()) {
            // There are more nodes in the tier (or more generic data nodes) than there are nodes that are being removed, so
            // there's at least one node that can hold data for the preferred tier that isn't being removed
            return true;
        }

        // A tier might be unavailable because all remaining nodes in the tier are being removed, so now we have to check if there are any
        // nodes with appropriate roles that aren't being removed.
        for (String nodeId : dataNodes) {
            if (removingNodeIds.contains(nodeId) == false) {
                return true;
            }
        }
        for (String nodeId : nodesWithTier) {
            if (removingNodeIds.contains(nodeId) == false) {
                return true;
            }
        }
        // All the nodes with roles appropriate for this tier are being removed, so this tier is not available.
        return false;
    }

    public static boolean allocationAllowed(String tierName, Set<DiscoveryNodeRole> roles) {
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

    private static Set<String> removingNodeIds(NodesShutdownMetadata shutdownMetadata) {
        if (shutdownMetadata.getAll().isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> removingNodes = new HashSet<>();
        for (var shutdownEntry : shutdownMetadata.getAll().values()) {
            if (shutdownEntry.getType().isRemovalType()) {
                removingNodes.add(shutdownEntry.getNodeId());
            }
        }
        return Collections.unmodifiableSet(removingNodes);
    }
}
