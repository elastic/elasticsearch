/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting.Role;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.util.Optional;

/**
 * Enforces project isolation for stateless {@link Role#INDEX_ONLY} and
 * {@link Role#SEARCH_ONLY} shards by forbidding isolated workloads on shared nodes.
 */
public class ProjectIsolationAllocationDecider extends AllocationDecider {

    public static final String NAME = "project_isolation";

    /**
     * Node attribute set on isolated tier nodes by the control plane. Shared tier nodes omit this attribute.
     */
    public static final String NODE_ATTRIBUTE_ISOLATED_TIER = "isolated_tier";

    public static final Setting<IsolatedProjects> CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING = new Setting<>(
        "cluster.routing.allocation.isolated_projects",
        "[]",
        IsolatedProjects::parse,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Decision YES_ISOLATION_MATCH = Decision.single(
        Type.YES,
        NAME,
        "shard project and tier match isolated tier for this node"
    );

    private volatile IsolatedProjects isolatedProjects = IsolatedProjects.EMPTY;

    public ProjectIsolationAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING,
            updatedIsolatedProjects -> this.isolatedProjects = updatedIsolatedProjects
        );
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode routingNode, RoutingAllocation routingAllocation) {
        return decide(shardRouting, routingNode, routingAllocation);
    }

    @Override
    public Decision canRemain(
        IndexMetadata indexMetadata,
        ShardRouting shardRouting,
        RoutingNode routingNode,
        RoutingAllocation routingAllocation
    ) {
        return decide(shardRouting, routingNode, routingAllocation);
    }

    private Decision decide(ShardRouting shardRouting, RoutingNode routingNode, RoutingAllocation routingAllocation) {
        IsolationShardTier isolationShardTier = IsolationShardTier.fromShardRole(shardRouting.role());

        // Project isolation applies only to stateless index vs search shard copies.
        if (isolationShardTier == null) {
            return Decision.ALWAYS;
        }

        ProjectId projectId = routingAllocation.metadata().projectFor(shardRouting.index()).id();
        Optional<String> optionalConfiguredIsolatedTierName = isolatedProjects.isolatedTierName(projectId, isolationShardTier);
        String nodeIsolatedTierAttributeValue = routingNode.node().getAttributes().get(NODE_ATTRIBUTE_ISOLATED_TIER);

        // This project is marked as isolated for at least one tier
        if (optionalConfiguredIsolatedTierName.isPresent()) {
            String configuredIsolatedTierName = optionalConfiguredIsolatedTierName.get();
            // The tier of the node matches the tier to be isolated
            if (configuredIsolatedTierName.equals(nodeIsolatedTierAttributeValue)) {
                return YES_ISOLATION_MATCH;
            }
            if (routingAllocation.debugDecision()) {
                return routingAllocation.decision(
                    Decision.NO,
                    NAME,
                    "shard requires isolated tier [%s] for project [%s] and tier [%s]; node isolated tier is [%s]",
                    configuredIsolatedTierName,
                    projectId.id(),
                    isolationShardTier.isolationTierName(),
                    nodeIsolatedTierAttributeValue
                );
            }
            return Decision.NO;
        }

        // The project is not marked as isolated.
        // If this is an isolated node, then we do not want to allocate non-isolated projects here
        if (nodeIsolatedTierAttributeValue != null) {
            if (routingAllocation.debugDecision()) {
                return routingAllocation.decision(
                    Decision.NO,
                    NAME,
                    "non-isolated shard cannot be assigned to isolated tier node [%s]",
                    nodeIsolatedTierAttributeValue
                );
            }
            return Decision.NO;
        }

        // The project is not marked as isolated
        // This is not an isolated node, so default to normal allocation
        return Decision.ALWAYS;
    }
}
