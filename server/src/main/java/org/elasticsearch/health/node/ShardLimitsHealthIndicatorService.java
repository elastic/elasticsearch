/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.indices.ShardLimitValidator;

import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.indices.ShardLimitValidator.FROZEN_GROUP;
import static org.elasticsearch.indices.ShardLimitValidator.NORMAL_GROUP;

/**
 *  This indicator reports health data about the shard limit across the cluster.
 *
 * <p>
 * The indicator will report:
 * * RED when there's room for less than 5 shards (either normal or frozen nodes)
 * * YELLOW when there's room for less than 10 shards (either normal or frozen nodes)
 * * GREEN otherwise
 * </p>
 *
 *  Although the `max_shard_per_node(.frozen)?` information is scoped by Node, we use the information from master because there is where
 *  the available room for new shards is checked before creating new indices.
 */
public class ShardLimitsHealthIndicatorService implements HealthIndicatorService {

    private static final String NAME = "shard_limits";
    private static final String UPGRADE_BLOCKED = "The cluster has too many used shards to be able to upgrade";
    private static final String UPGRADE_AT_RISK = "The cluster is running low on room to add new shards hence upgrade is at risk";
    private static final String HELP_GUIDE = "https://ela.st/max-shard-limit-reached";
    static final List<HealthIndicatorImpact> RED_INDICATOR_IMPACTS = List.of(
        new HealthIndicatorImpact(NAME, "upgrade_blocked", 1, UPGRADE_BLOCKED, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT))
    );
    static final List<HealthIndicatorImpact> YELLOW_INDICATOR_IMPACTS = List.of(
        new HealthIndicatorImpact(NAME, "upgrade_at_risk", 2, UPGRADE_AT_RISK, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT))
    );

    static final Diagnosis SHARD_LIMITS_REACHED_NORMAL_NODES = new Diagnosis(
        new Diagnosis.Definition(
            NAME,
            "increase_max_shards_per_node",
            "The current value of `"
                + ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey()
                + "` does not allow to add more shards to the cluster.",
            "Consider increasing the currently set value or remove indices to clear up resources " + HELP_GUIDE,
            HELP_GUIDE
        ),
        null
    );
    static final Diagnosis SHARD_LIMITS_REACHED_FROZEN_NODES = new Diagnosis(
        new Diagnosis.Definition(
            NAME,
            "increase_max_shards_per_node_frozen",
            "The current value of `"
                + ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey()
                + "` does not allow to add more shards to the cluster.",
            "Consider increasing the currently set value or remove indices to clear up resources " + HELP_GUIDE,
            HELP_GUIDE
        ),
        null
    );

    private final ClusterService clusterService;

    public ShardLimitsHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        var healthMetadata = HealthMetadata.getFromClusterState(clusterService.state());
        if (healthMetadata == null || healthMetadata.getShardLimitsMetadata() == null) {
            return unknownIndicator();
        }

        return mergeIndicators(
            calculateForNormalNodes(healthMetadata.getShardLimitsMetadata()),
            calculateForFrozenNodes(healthMetadata.getShardLimitsMetadata())
        );
    }

    private HealthIndicatorResult mergeIndicators(StatusResult normalNodes, StatusResult frozenNodes) {
        HealthStatus finalStatus = HealthStatus.merge(Stream.of(normalNodes.status, frozenNodes.status));
        List<Diagnosis> diagnoses = List.of();
        var symptomBuilder = new StringBuilder();

        if (finalStatus == HealthStatus.GREEN) {
            symptomBuilder.append("The cluster has enough room to add new shards.");
        }

        // RED and YELLOW status indicates that the cluster might have issues. finalStatus has the worst between *normal and frozen* nodes,
        // so we have to check each of the groups in order of provide the right message.
        if (finalStatus.indicatesHealthProblem()) {
            symptomBuilder.append("Cluster is close to reaching the maximum number of shards for ");
            if (normalNodes.status == frozenNodes.status) {
                symptomBuilder.append(NORMAL_GROUP).append(" and ").append(FROZEN_GROUP);
                diagnoses = List.of(SHARD_LIMITS_REACHED_NORMAL_NODES, SHARD_LIMITS_REACHED_FROZEN_NODES);

            } else if (normalNodes.status.indicatesHealthProblem()) {
                symptomBuilder.append(NORMAL_GROUP);
                diagnoses = List.of(SHARD_LIMITS_REACHED_NORMAL_NODES);

            } else if (frozenNodes.status.indicatesHealthProblem()) {
                symptomBuilder.append(FROZEN_GROUP);
                diagnoses = List.of(SHARD_LIMITS_REACHED_FROZEN_NODES);
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
            buildDetails(normalNodes.result, frozenNodes.result),
            indicatorImpacts,
            diagnoses
        );
    }

    private StatusResult calculateForNormalNodes(HealthMetadata.ShardLimits shardLimits) {
        int maxShardsPerNodeSetting = shardLimits.maxShardsPerNode();
        var result = ShardLimitValidator.checkShardLimitForNormalNodes(maxShardsPerNodeSetting, 5, 1, clusterService.state());

        if (result.canAddShards() == false) {
            return new StatusResult(HealthStatus.RED, result);
        }

        result = ShardLimitValidator.checkShardLimitForNormalNodes(maxShardsPerNodeSetting, 10, 1, clusterService.state());
        if (result.canAddShards() == false) {
            return new StatusResult(HealthStatus.YELLOW, result);
        }

        return new StatusResult(HealthStatus.GREEN, result);
    }

    private StatusResult calculateForFrozenNodes(HealthMetadata.ShardLimits shardLimits) {
        int maxShardsPerNodeSetting = shardLimits.maxShardsPerNodeFrozen();
        var result = ShardLimitValidator.checkShardLimitForFrozenNodes(maxShardsPerNodeSetting, 5, 1, clusterService.state());
        if (result.canAddShards() == false) {
            return new StatusResult(HealthStatus.RED, result);
        }

        result = ShardLimitValidator.checkShardLimitForFrozenNodes(maxShardsPerNodeSetting, 10, 1, clusterService.state());
        if (result.canAddShards() == false) {
            return new StatusResult(HealthStatus.YELLOW, result);
        }

        return new StatusResult(HealthStatus.GREEN, result);
    }

    static HealthIndicatorDetails buildDetails(ShardLimitValidator.Result normalNodes, ShardLimitValidator.Result frozenNodes) {
        return (builder, params) -> {
            builder.startObject();
            {
                builder.startObject(NORMAL_GROUP + "_nodes");
                builder.field("max_shards_in_cluster", normalNodes.maxShardsInCluster());
                if (normalNodes.currentUsedShards().isPresent()) {
                    builder.field("current_used_shards", normalNodes.currentUsedShards().get());
                }
                builder.endObject();
            }
            {
                builder.startObject(FROZEN_GROUP + "_nodes");
                builder.field("max_shards_in_cluster", frozenNodes.maxShardsInCluster());
                if (frozenNodes.currentUsedShards().isPresent()) {
                    builder.field("current_used_shards", frozenNodes.currentUsedShards().get());
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        };
    }

    private HealthIndicatorResult unknownIndicator() {
        return createIndicator(HealthStatus.UNKNOWN, "No shard limits data.", HealthIndicatorDetails.EMPTY, List.of(), List.of());
    }

    private record StatusResult(HealthStatus status, ShardLimitValidator.Result result) {}
}
