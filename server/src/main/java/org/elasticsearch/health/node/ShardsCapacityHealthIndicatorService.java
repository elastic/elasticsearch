/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.indices.ShardLimitValidator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 *  This indicator reports health data about the shard capacity across the cluster.
 *
 * The indicator will report:
 * <ul>
 * <li> {@code RED} when there's room for less than the configured {@code health.shard_capacity.unhealthy_threshold.red} (default 5) shards
 * (either data or frozen nodes)</li>
 * <li> {@code YELLOW} when there's room for less than the configured {@code health.shard_capacity.unhealthy_threshold.yellow} (default 10)
 * shards (either data or frozen nodes)</li>
 * <li> {@code GREEN} otherwise</li>
 * </ul>
 *
 *  Although the `max_shard_per_node(.frozen)?` information is scoped by Node, we use the information from master because there is where
 *  the available room for new shards is checked before creating new indices.
 */
public class ShardsCapacityHealthIndicatorService implements HealthIndicatorService {

    static final String NAME = "shards_capacity";

    private static final String UPGRADE_BLOCKED = "The cluster has too many used shards to be able to upgrade.";
    private static final String UPGRADE_AT_RISK =
        "The cluster is running low on room to add new shard. Upgrading to a new version is at risk.";
    private static final String INDEX_CREATION_BLOCKED =
        "The cluster is running low on room to add new shards. Adding data to new indices is at risk";
    private static final String INDEX_CREATION_RISK =
        "The cluster is running low on room to add new shards. Adding data to new indices might soon fail.";
    private static final TriFunction<String, Setting<?>, String, Diagnosis> SHARD_MAX_CAPACITY_REACHED_FN = (
        id,
        setting,
        indexType) -> new Diagnosis(
            new Diagnosis.Definition(
                NAME,
                id,
                "Elasticsearch is about to reach the maximum number of shards it can host as set by [" + setting.getKey() + "].",
                "Increase the number of nodes in your cluster or remove some "
                    + indexType
                    + " indices to reduce the number of shards in the cluster.",
                ReferenceDocs.CLUSTER_SHARD_LIMIT.toString()
            ),
            null
        );

    static final List<HealthIndicatorImpact> RED_INDICATOR_IMPACTS = List.of(
        new HealthIndicatorImpact(NAME, "upgrade_blocked", 1, UPGRADE_BLOCKED, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)),
        new HealthIndicatorImpact(NAME, "creation_of_new_indices_blocked", 1, INDEX_CREATION_BLOCKED, List.of(ImpactArea.INGEST))
    );
    static final List<HealthIndicatorImpact> YELLOW_INDICATOR_IMPACTS = List.of(
        new HealthIndicatorImpact(NAME, "upgrade_at_risk", 2, UPGRADE_AT_RISK, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)),
        new HealthIndicatorImpact(NAME, "creation_of_new_indices_at_risk", 2, INDEX_CREATION_RISK, List.of(ImpactArea.INGEST))
    );
    static final Diagnosis SHARDS_MAX_CAPACITY_REACHED_DATA_NODES = SHARD_MAX_CAPACITY_REACHED_FN.apply(
        "decrease_shards_per_non_frozen_node",
        ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE,
        "non-frozen"
    );
    static final Diagnosis SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES = SHARD_MAX_CAPACITY_REACHED_FN.apply(
        "decrease_shards_per_frozen_node",
        ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN,
        "frozen"
    );

    public static final Setting<Integer> SETTING_SHARD_CAPACITY_UNHEALTHY_THRESHOLD_YELLOW = Setting.intSetting(
        "health.shard_capacity.unhealthy_threshold.yellow",
        10,
        1,
        new Setting.Validator<>() {
            @Override
            public void validate(Integer value) {}

            @Override
            public void validate(Integer value, Map<Setting<?>, Object> settings) {
                Integer redThreshold = (Integer) settings.get(SETTING_SHARD_CAPACITY_UNHEALTHY_THRESHOLD_RED);
                if (value <= redThreshold) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Setting [%s] (%d) must be greater than [%s] (%d)",
                            SETTING_SHARD_CAPACITY_UNHEALTHY_THRESHOLD_YELLOW.getKey(),
                            value,
                            SETTING_SHARD_CAPACITY_UNHEALTHY_THRESHOLD_RED.getKey(),
                            redThreshold
                        )
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(SETTING_SHARD_CAPACITY_UNHEALTHY_THRESHOLD_RED);
                return settings.iterator();
            }
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> SETTING_SHARD_CAPACITY_UNHEALTHY_THRESHOLD_RED = Setting.intSetting(
        "health.shard_capacity.unhealthy_threshold.red",
        5,
        1,
        new Setting.Validator<>() {
            @Override
            public void validate(Integer value) {}

            @Override
            public void validate(Integer value, Map<Setting<?>, Object> settings) {
                Integer yellowThreshold = (Integer) settings.get(SETTING_SHARD_CAPACITY_UNHEALTHY_THRESHOLD_YELLOW);
                if (value >= yellowThreshold) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Setting [%s] (%d) must be less than [%s] (%d)",
                            SETTING_SHARD_CAPACITY_UNHEALTHY_THRESHOLD_RED.getKey(),
                            value,
                            SETTING_SHARD_CAPACITY_UNHEALTHY_THRESHOLD_YELLOW.getKey(),
                            yellowThreshold
                        )
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(SETTING_SHARD_CAPACITY_UNHEALTHY_THRESHOLD_YELLOW);
                return settings.iterator();
            }
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final List<ShardLimitValidator.LimitGroup> shardLimitGroups;

    public ShardsCapacityHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.shardLimitGroups = ShardLimitValidator.applicableLimitGroups(DiscoveryNode.isStateless(clusterService.getSettings()));
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        var state = clusterService.state();
        var healthMetadata = HealthMetadata.getFromClusterState(state);
        if (healthMetadata == null || healthMetadata.getShardLimitsMetadata() == null) {
            return unknownIndicator();
        }

        var shardLimitsMetadata = healthMetadata.getShardLimitsMetadata();
        final List<StatusResult> statusResults = shardLimitGroups.stream()
            .map(
                limitGroup -> calculateFrom(
                    ShardLimitValidator.getShardLimitPerNode(limitGroup, shardLimitsMetadata),
                    state.nodes(),
                    state.metadata(),
                    limitGroup::checkShardLimit,
                    healthMetadata.getShardLimitsMetadata().shardCapacityUnhealthyThresholdYellow(),
                    healthMetadata.getShardLimitsMetadata().shardCapacityUnhealthyThresholdRed()
                )
            )
            .toList();

        return mergeIndicators(verbose, statusResults, healthMetadata);
    }

    private HealthIndicatorResult mergeIndicators(boolean verbose, List<StatusResult> statusResults, HealthMetadata healthMetadata) {
        var finalStatus = HealthStatus.merge(statusResults.stream().map(StatusResult::status));
        var diagnoses = new LinkedHashSet<Diagnosis>();
        var symptomBuilder = new StringBuilder();

        if (finalStatus == HealthStatus.GREEN) {
            symptomBuilder.append("The cluster has enough room to add new shards.");
        }

        // RED and YELLOW status indicates that the cluster might have issues. finalStatus has the worst between *data (non-frozen) and
        // frozen* nodes, so we have to check each of the groups in order of provide the right message.
        if (finalStatus.indicatesHealthProblem()) {
            symptomBuilder.append("Cluster is close to reaching the configured maximum number of shards for ");
            final var nodeTypeNames = new ArrayList<String>();
            for (var statusResult : statusResults) {
                if (statusResult.status.indicatesHealthProblem()) {
                    nodeTypeNames.add(nodeTypeFroLimitGroup(statusResult.result.group()));
                    diagnoses.add(diagnosisForLimitGroup(statusResult.result.group()));
                }
            }

            assert nodeTypeNames.isEmpty() == false;
            symptomBuilder.append(nodeTypeNames.getFirst());
            for (int i = 1; i < nodeTypeNames.size() - 1; i++) {
                symptomBuilder.append(", ").append(nodeTypeNames.get(i));
            }
            if (nodeTypeNames.size() > 1) {
                symptomBuilder.append(" and ").append(nodeTypeNames.getLast());
            }
            symptomBuilder.append(" nodes.");
        }

        var indicatorImpacts = switch (finalStatus) {
            case RED -> RED_INDICATOR_IMPACTS;
            case YELLOW -> YELLOW_INDICATOR_IMPACTS;
            default -> List.<HealthIndicatorImpact>of();
        };

        return createIndicator(
            finalStatus,
            symptomBuilder.toString(),
            verbose ? buildDetails(statusResults.stream().map(StatusResult::result).toList()) : HealthIndicatorDetails.EMPTY,
            indicatorImpacts,
            verbose ? List.copyOf(diagnoses) : List.of()
        );
    }

    static StatusResult calculateFrom(
        int maxShardsPerNodeSetting,
        DiscoveryNodes discoveryNodes,
        Metadata metadata,
        ShardsCapacityChecker checker,
        int shardThresholdYellow,
        int shardThresholdRed
    ) {
        var result = checker.check(maxShardsPerNodeSetting, shardThresholdRed, 1, discoveryNodes, metadata);
        if (result.canAddShards() == false) {
            return new StatusResult(HealthStatus.RED, result);
        }

        result = checker.check(maxShardsPerNodeSetting, shardThresholdYellow, 1, discoveryNodes, metadata);
        if (result.canAddShards() == false) {
            return new StatusResult(HealthStatus.YELLOW, result);
        }

        return new StatusResult(HealthStatus.GREEN, result);
    }

    static HealthIndicatorDetails buildDetails(List<ShardLimitValidator.Result> results) {
        return (builder, params) -> {
            builder.startObject();
            for (var result : results) {
                builder.startObject(nodeTypeFroLimitGroup(result.group()));
                builder.field("max_shards_in_cluster", result.maxShardsInCluster());
                if (result.currentUsedShards().isPresent()) {
                    builder.field("current_used_shards", result.currentUsedShards().get());
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        };
    }

    private HealthIndicatorResult unknownIndicator() {
        return createIndicator(
            HealthStatus.UNKNOWN,
            "Unable to determine shard capacity status.",
            HealthIndicatorDetails.EMPTY,
            List.of(),
            List.of()
        );
    }

    private static String nodeTypeFroLimitGroup(ShardLimitValidator.LimitGroup limitGroup) {
        return switch (limitGroup) {
            case NORMAL -> "data";
            case FROZEN -> "frozen";
            case INDEX -> "index";
            case SEARCH -> "search";
        };
    }

    private static Diagnosis diagnosisForLimitGroup(ShardLimitValidator.LimitGroup limitGroup) {
        return switch (limitGroup) {
            case NORMAL, INDEX, SEARCH -> SHARDS_MAX_CAPACITY_REACHED_DATA_NODES;
            case FROZEN -> SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES;
        };
    }

    record StatusResult(HealthStatus status, ShardLimitValidator.Result result) {}

    @FunctionalInterface
    interface ShardsCapacityChecker {
        ShardLimitValidator.Result check(
            int maxConfiguredShardsPerNode,
            int numberOfNewShards,
            int replicas,
            DiscoveryNodes discoveryNodes,
            Metadata metadata
        );
    }
}
