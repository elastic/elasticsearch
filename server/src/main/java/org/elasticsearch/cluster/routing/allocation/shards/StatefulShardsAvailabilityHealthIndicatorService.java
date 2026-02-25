/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.indices.SystemIndices;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;

/**
 * This indicator reports health for shards.
 * <p>
 * Indicator will report:
 * * RED when one or more primary shards are not available
 * * YELLOW when one or more replica shards are not available
 * * GREEN otherwise
 * <p>
 * Each shard needs to be available and replicated in order to guarantee high availability and prevent data loses.
 * Shards allocated on nodes scheduled for restart (using nodes shutdown API) will not degrade this indicator health.
 */
public class StatefulShardsAvailabilityHealthIndicatorService extends ShardsAvailabilityHealthIndicatorService
    implements
        HealthIndicatorService {

    private static final String DATA_TIER_ALLOCATION_DECIDER_NAME = "data_tier";

    public StatefulShardsAvailabilityHealthIndicatorService(
        ClusterService clusterService,
        AllocationService allocationService,
        SystemIndices systemIndices,
        ProjectResolver projectResolver
    ) {
        super(clusterService, allocationService, systemIndices, projectResolver);
    }

    @Override
    public ShardAllocationStatus createNewStatus(Metadata metadata, int maxAffectedResourcesCount) {
        return new StatefulShardAllocationStatus(metadata, maxAffectedResourcesCount);
    }

    private static final Map<String, Diagnosis.Definition> ACTION_ENABLE_TIERS_LOOKUP = DataTier.ALL_DATA_TIERS.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    NAME,
                    "enable_data_tiers:tier:" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices because the indices expect to be allocated to "
                        + "data tier nodes, but there were not any nodes with the expected tiers found in the cluster.",
                    "Add nodes with the [" + tier + "] role to the cluster.",
                    ENABLE_TIER_ACTION_GUIDE
                )
            )
        );

    private static final Map<String, Diagnosis.Definition> ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING_LOOKUP = DataTier.ALL_DATA_TIERS
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    NAME,
                    "increase_shard_limit_index_setting:tier:" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices because each node in the ["
                        + tier
                        + "] tier has reached the index shard limit.",
                    "Increase the values for the ["
                        + INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()
                        + "] index setting on each index or add more nodes to the target tiers.",
                    INCREASE_SHARD_LIMIT_ACTION_GUIDE
                )
            )
        );

    private static final Map<String, Diagnosis.Definition> ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING_LOOKUP = DataTier.ALL_DATA_TIERS
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    NAME,
                    "increase_shard_limit_cluster_setting:tier:" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices because each node in the ["
                        + tier
                        + "] tier has reached the cluster shard limit.",
                    "Increase the values for the ["
                        + CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey()
                        + "] cluster setting or add more nodes to the target tiers.",
                    INCREASE_CLUSTER_SHARD_LIMIT_ACTION_GUIDE
                )
            )
        );

    public static final String MIGRATE_TO_TIERS_ACTION_GUIDE = "https://ela.st/migrate-to-tiers";
    public static final Diagnosis.Definition ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA = new Diagnosis.Definition(
        NAME,
        "migrate_data_tiers_require_data",
        "Elasticsearch isn't allowed to allocate some shards from these indices to any nodes in the desired data tiers because the "
            + "indices are configured with allocation filter rules that are incompatible with the nodes in this tier.",
        "Remove ["
            + INDEX_ROUTING_REQUIRE_GROUP_PREFIX
            + ".data] from the index settings or try migrating to data tiers by first stopping ILM [POST /_ilm/stop] and then using "
            + "the data tier migration action [POST /_ilm/migrate_to_data_tiers]. Finally, restart ILM [POST /_ilm/start].",
        MIGRATE_TO_TIERS_ACTION_GUIDE
    );

    public static final Map<String, Diagnosis.Definition> ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA_LOOKUP = DataTier.ALL_DATA_TIERS
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    NAME,
                    "migrate_data_tiers_require_data:tier:" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices to any nodes in the ["
                        + tier
                        + "] data tier because the indices are configured with allocation filter rules that are incompatible with the "
                        + "nodes in this tier.",
                    "Remove ["
                        + INDEX_ROUTING_REQUIRE_GROUP_PREFIX
                        + ".data] from the index settings or try migrating to data tiers by first stopping ILM [POST /_ilm/stop] and then "
                        + "using the data tier migration action [POST /_ilm/migrate_to_data_tiers]. "
                        + "Finally, restart ILM [POST /_ilm/start].",
                    MIGRATE_TO_TIERS_ACTION_GUIDE
                )
            )
        );

    public static final Diagnosis.Definition ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA = new Diagnosis.Definition(
        NAME,
        "migrate_data_tiers_include_data",
        "Elasticsearch isn't allowed to allocate some shards from these indices to any nodes in the desired data tiers because the "
            + "indices are configured with allocation filter rules that are incompatible with the nodes in this tier.",
        "Remove ["
            + INDEX_ROUTING_INCLUDE_GROUP_PREFIX
            + ".data] from the index settings or try migrating to data tiers by first stopping ILM [POST /_ilm/stop] and then using "
            + "the data tier migration action [POST /_ilm/migrate_to_data_tiers]. Finally, restart ILM [POST /_ilm/start].",
        MIGRATE_TO_TIERS_ACTION_GUIDE
    );

    public static final Map<String, Diagnosis.Definition> ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA_LOOKUP = DataTier.ALL_DATA_TIERS
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    NAME,
                    "migrate_data_tiers_include_data:tier:" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices to any nodes in the ["
                        + tier
                        + "] data tier because the indices are configured with allocation filter rules that are incompatible with the "
                        + "nodes in this tier.",
                    "Remove ["
                        + INDEX_ROUTING_INCLUDE_GROUP_PREFIX
                        + ".data] from the index settings or try migrating to data tiers by first stopping ILM [POST /_ilm/stop] and then "
                        + "using the data tier migration action [POST /_ilm/migrate_to_data_tiers]. Finally, restart ILM "
                        + "[POST /_ilm/start].",
                    MIGRATE_TO_TIERS_ACTION_GUIDE
                )
            )
        );

    // Visible for testing
    public static final Map<String, Diagnosis.Definition> ACTION_INCREASE_TIER_CAPACITY_LOOKUP = DataTier.ALL_DATA_TIERS.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    NAME,
                    "increase_tier_capacity_for_allocations:tier:" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices to any of the nodes in the desired data tier "
                        + "because there are not enough nodes in the ["
                        + tier
                        + "] tier to allocate each shard copy on a different node.",
                    "Increase the number of nodes in this tier or decrease the number of replica shards in the affected indices.",
                    TIER_CAPACITY_ACTION_GUIDE
                )
            )
        );

    /**
     * Generates a list of diagnoses for common problems that keep a shard from allocating to nodes depending on their role;
     * a very common example of such a case are data tiers.
     * @param indexMetadata Index metadata for the shard being diagnosed.
     * @param nodeAllocationResults allocation decision results for all nodes in the cluster.
     * @param clusterState the current cluster state.
     * @param shardRouting the shard the nodeAllocationResults refer to
     * @return A list of diagnoses for the provided unassigned shard
     */
    @Override
    protected List<Diagnosis.Definition> checkNodeRoleRelatedIssues(
        IndexMetadata indexMetadata,
        List<NodeAllocationResult> nodeAllocationResults,
        ClusterState clusterState,
        ShardRouting shardRouting
    ) {
        List<Diagnosis.Definition> diagnosisDefs = new ArrayList<>();
        if (indexMetadata.getTierPreference().isEmpty() == false) {
            List<NodeAllocationResult> dataTierAllocationResults = nodeAllocationResults.stream()
                .filter(hasDeciderResult(DATA_TIER_ALLOCATION_DECIDER_NAME, Decision.Type.YES))
                .toList();
            if (dataTierAllocationResults.isEmpty()) {
                // Shard must be allocated on specific tiers but no nodes were enabled for those tiers.
                for (String tier : indexMetadata.getTierPreference()) {
                    Optional.ofNullable(getAddNodesWithRoleAction(tier)).ifPresent(diagnosisDefs::add);
                }
            } else {
                // Collect the nodes from the tiers this index is allowed on
                Set<DiscoveryNode> dataTierNodes = dataTierAllocationResults.stream()
                    .map(NodeAllocationResult::getNode)
                    .collect(Collectors.toSet());

                // Determine the unique roles available on the allowed tier nodes
                Set<String> dataTierRolesAvailable = dataTierNodes.stream()
                    .map(DiscoveryNode::getRoles)
                    .flatMap(Set::stream)
                    .map(DiscoveryNodeRole::roleName)
                    .collect(Collectors.toSet());

                // Determine which tier this index would most prefer to live on
                String preferredTier = indexMetadata.getTierPreference()
                    .stream()
                    .filter(dataTierRolesAvailable::contains)
                    .findFirst()
                    .orElse(null);

                // Run checks for data tier specific problems
                diagnosisDefs.addAll(
                    checkNodesWithRoleAtShardLimit(indexMetadata, clusterState, dataTierAllocationResults, dataTierNodes, preferredTier)
                );
                diagnosisDefs.addAll(checkDataTierShouldMigrate(indexMetadata, dataTierAllocationResults, preferredTier, dataTierNodes));
                checkNotEnoughNodesWithRole(dataTierAllocationResults, preferredTier).ifPresent(diagnosisDefs::add);
            }
        }
        return diagnosisDefs;
    }

    private static List<Diagnosis.Definition> checkDataTierShouldMigrate(
        IndexMetadata indexMetadata,
        List<NodeAllocationResult> dataTierAllocationResults,
        @Nullable String preferredTier,
        Set<DiscoveryNode> dataTierNodes
    ) {
        // Check if index has filter requirements on the old "data" attribute that might be keeping it from allocating.
        if (dataTierAllocationResults.stream().allMatch(hasDeciderResult(FilterAllocationDecider.NAME, Decision.Type.NO))) {
            List<Diagnosis.Definition> diagnosisDefs = new ArrayList<>();
            Map<String, List<String>> requireAttributes = INDEX_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(indexMetadata.getSettings());
            List<String> requireDataAttributes = requireAttributes.get("data");
            DiscoveryNodeFilters requireFilter = requireDataAttributes == null
                ? null
                : DiscoveryNodeFilters.buildFromKeyValues(DiscoveryNodeFilters.OpType.AND, Map.of("data", requireDataAttributes));

            Map<String, List<String>> includeAttributes = INDEX_ROUTING_INCLUDE_GROUP_SETTING.getAsMap(indexMetadata.getSettings());
            List<String> includeDataAttributes = includeAttributes.get("data");
            DiscoveryNodeFilters includeFilter = includeDataAttributes == null
                ? null
                : DiscoveryNodeFilters.buildFromKeyValues(DiscoveryNodeFilters.OpType.OR, Map.of("data", includeDataAttributes));
            if (requireFilter != null || includeFilter != null) {
                // Check if the data tier nodes this shard is allowed on have data attributes that match
                if (requireFilter != null && dataTierNodes.stream().noneMatch(requireFilter::match)) {
                    // No data tier nodes match the required data attribute
                    diagnosisDefs.add(
                        Optional.ofNullable(preferredTier)
                            .map(ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA_LOOKUP::get)
                            .orElse(ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA)
                    );
                }
                if (includeFilter != null && dataTierNodes.stream().noneMatch(includeFilter::match)) {
                    // No data tier nodes match the included data attributes
                    diagnosisDefs.add(
                        Optional.ofNullable(preferredTier)
                            .map(ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA_LOOKUP::get)
                            .orElse(ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA)
                    );
                }
            }
            return diagnosisDefs;
        } else {
            return List.of();
        }
    }

    @Override
    public Diagnosis.Definition getAddNodesWithRoleAction(String role) {
        return ACTION_ENABLE_TIERS_LOOKUP.get(role);
    }

    @Override
    public Diagnosis.Definition getIncreaseShardLimitIndexSettingAction(String role) {
        return ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING_LOOKUP.get(role);
    }

    @Override
    public Diagnosis.Definition getIncreaseShardLimitClusterSettingAction(String role) {
        return ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING_LOOKUP.get(role);
    }

    @Override
    public Diagnosis.Definition getIncreaseNodeWithRoleCapacityAction(String role) {
        return ACTION_INCREASE_TIER_CAPACITY_LOOKUP.get(role);
    }

    public class StatefulShardAllocationStatus extends ShardsAvailabilityHealthIndicatorService.ShardAllocationStatus {

        public StatefulShardAllocationStatus(Metadata clusterMetadata, int maxAffectedResourcesCount) {
            super(clusterMetadata, maxAffectedResourcesCount);
        }

        @Override
        public HealthStatus getStatus() {
            if (primaries.areAllAvailable() == false || primaries.searchableSnapshotsState.getRedSearchableSnapshots().isEmpty() == false) {
                return RED;
            } else if (replicas.areAllAvailable() == false) {
                return YELLOW;
            } else {
                return GREEN;
            }
        }
    }
}
