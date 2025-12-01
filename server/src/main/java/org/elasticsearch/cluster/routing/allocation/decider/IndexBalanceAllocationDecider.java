/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.IndexBalanceConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.SEARCH_ROLE;
import static org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING;

/**
 * For an index of n shards hosted by a cluster of m nodes, a node should not host
 * significantly more than n / m shards. This allocation decider enforces this principle.
 * This allocation decider excludes any nodes flagged for shutdown from consideration
 * when computing optimal shard distributions.
 */
public class IndexBalanceAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(IndexBalanceAllocationDecider.class);
    private static final String EMPTY = "";

    public static final String NAME = "index_balance";

    private final IndexBalanceConstraintSettings indexBalanceConstraintSettings;
    private final boolean isStateless;

    private volatile DiscoveryNodeFilters clusterRequireFilters;
    private volatile DiscoveryNodeFilters clusterIncludeFilters;
    private volatile DiscoveryNodeFilters clusterExcludeFilters;

    public IndexBalanceAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.indexBalanceConstraintSettings = new IndexBalanceConstraintSettings(clusterSettings);
        setClusterRequireFilters(CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(settings));
        setClusterExcludeFilters(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getAsMap(settings));
        setClusterIncludeFilters(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getAsMap(settings));
        clusterSettings.addAffixMapUpdateConsumer(CLUSTER_ROUTING_REQUIRE_GROUP_SETTING, this::setClusterRequireFilters, (a, b) -> {});
        clusterSettings.addAffixMapUpdateConsumer(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING, this::setClusterExcludeFilters, (a, b) -> {});
        clusterSettings.addAffixMapUpdateConsumer(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING, this::setClusterIncludeFilters, (a, b) -> {});
        isStateless = DiscoveryNode.isStateless(settings);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (indexBalanceConstraintSettings.isDeciderEnabled() == false || isStateless == false || hasFilters()) {
            return allocation.decision(Decision.YES, NAME, "Decider is disabled.");
        }

        Index index = shardRouting.index();
        if (node.hasIndex(index) == false) {
            return allocation.decision(Decision.YES, NAME, "Node does not currently host this index.");
        }

        assert node.node() != null;
        assert node.node().getRoles().contains(INDEX_ROLE) || node.node().getRoles().contains(SEARCH_ROLE);

        if (node.node().getRoles().contains(INDEX_ROLE) && shardRouting.primary() == false) {
            return allocation.decision(Decision.YES, NAME, "An index node cannot own search shards. Decider inactive.");
        }

        if (node.node().getRoles().contains(SEARCH_ROLE) && shardRouting.primary()) {
            return allocation.decision(Decision.YES, NAME, "A search node cannot own primary shards. Decider inactive.");
        }

        final ProjectId projectId = allocation.getClusterState().metadata().projectFor(index).id();
        final IndexMetadata indexMetadata = allocation.getClusterState().metadata().getProject(projectId).index(index);

        if (hasIndexRoutingFilters(indexMetadata)) {
            return allocation.decision(Decision.YES, NAME, "Decider is disabled for index level allocation filters.");
        }

        final Set<DiscoveryNode> eligibleNodes = new HashSet<>();
        int totalShards = 0;
        String nomenclature = EMPTY;

        if (node.node().getRoles().contains(INDEX_ROLE)) {
            collectEligibleNodes(allocation, eligibleNodes, INDEX_ROLE);
            // Primary shards only.
            totalShards = allocation.getClusterState().routingTable(projectId).index(index).size();
            nomenclature = "index";
        } else if (node.node().getRoles().contains(SEARCH_ROLE)) {
            collectEligibleNodes(allocation, eligibleNodes, SEARCH_ROLE);
            // Replicas only.
            totalShards = indexMetadata.getNumberOfShards() * indexMetadata.getNumberOfReplicas();
            nomenclature = "search";
        }

        assert eligibleNodes.isEmpty() == false;
        if (eligibleNodes.isEmpty()) {
            return allocation.decision(Decision.YES, NAME, "There are no eligible nodes available.");
        }
        assert totalShards > 0;
        final double idealAllocation = Math.ceil((double) totalShards / eligibleNodes.size());

        // Adding the excess shards before division ensures that with tolerance 1 we get:
        // 2 shards, 2 nodes, allow 2 on each
        // 3 shards, 2 nodes, allow 2 on each etc.
        final int threshold = Math.ceilDiv(totalShards + indexBalanceConstraintSettings.getExcessShards(), eligibleNodes.size());
        final int currentAllocation = node.numberOfOwningShardsForIndex(index);

        if (currentAllocation >= threshold) {
            String explanation = Strings.format(
                "There are [%d] eligible nodes in the [%s] tier for assignment of [%d] shards in index [%s]. Ideally no more than [%.0f] "
                    + "shard would be assigned per node (the index balance excess shards setting is [%d]). This node is already assigned"
                    + " [%d] shards of the index.",
                eligibleNodes.size(),
                nomenclature,
                totalShards,
                index,
                idealAllocation,
                indexBalanceConstraintSettings.getExcessShards(),
                currentAllocation
            );

            logger.trace(explanation);

            return allocation.decision(Decision.NOT_PREFERRED, NAME, explanation);
        }

        return allocation.decision(Decision.YES, NAME, "Node index shard allocation is under the threshold.");
    }

    private void collectEligibleNodes(RoutingAllocation allocation, Set<DiscoveryNode> eligibleNodes, DiscoveryNodeRole role) {
        for (DiscoveryNode discoveryNode : allocation.nodes()) {
            if (discoveryNode.getRoles().contains(role) && allocation.metadata().nodeShutdowns().contains(discoveryNode.getId()) == false) {
                eligibleNodes.add(discoveryNode);
            }
        }
    }

    private void setClusterRequireFilters(Map<String, List<String>> filters) {
        clusterRequireFilters = DiscoveryNodeFilters.trimTier(DiscoveryNodeFilters.buildFromKeyValues(AND, filters));
    }

    private void setClusterIncludeFilters(Map<String, List<String>> filters) {
        clusterIncludeFilters = DiscoveryNodeFilters.trimTier(DiscoveryNodeFilters.buildFromKeyValues(OR, filters));
    }

    private void setClusterExcludeFilters(Map<String, List<String>> filters) {
        clusterExcludeFilters = DiscoveryNodeFilters.trimTier(DiscoveryNodeFilters.buildFromKeyValues(OR, filters));
    }

    private boolean hasFilters() {
        return (clusterExcludeFilters != null && clusterExcludeFilters.hasFilters())
            || (clusterIncludeFilters != null && clusterIncludeFilters.hasFilters())
            || (clusterRequireFilters != null && clusterRequireFilters.hasFilters());
    }

    private boolean hasIndexRoutingFilters(IndexMetadata indexMetadata) {
        return DiscoveryNodeFilters.trimTier(indexMetadata.requireFilters()) != null
            && DiscoveryNodeFilters.trimTier(indexMetadata.requireFilters()).hasFilters()
            || DiscoveryNodeFilters.trimTier(indexMetadata.excludeFilters()) != null
                && DiscoveryNodeFilters.trimTier(indexMetadata.excludeFilters()).hasFilters()
            || DiscoveryNodeFilters.trimTier(indexMetadata.includeFilters()) != null
                && DiscoveryNodeFilters.trimTier(indexMetadata.includeFilters()).hasFilters();
    }
}
