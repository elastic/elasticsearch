/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
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
        var symptomBuilder = new StringBuilder();

        if (finalStatus == HealthStatus.GREEN) {
            symptomBuilder.append("The cluster has enough room to add new shards.");
        }

        if (finalStatus == HealthStatus.RED) {
            symptomBuilder.append("Cluster is close to reaching the maximum of shards for ");
            if (normalNodes.status == frozenNodes.status) {
                symptomBuilder.append("normal and frozen");
            } else if (normalNodes.status == HealthStatus.RED) {
                symptomBuilder.append(NORMAL_GROUP);
            } else {
                symptomBuilder.append(FROZEN_GROUP);
            }
            symptomBuilder.append(" nodes.");
        }

        return createIndicator(
            finalStatus,
            symptomBuilder.toString(),
            buildDetails(normalNodes.result, frozenNodes.result),
            List.of(),
            List.of()
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
                builder.startObject("normal_nodes");
                builder.field("max_shards_in_cluster", normalNodes.maxShardsInCluster());
                if (normalNodes.currentUsedShards().isPresent()) {
                    builder.field("current_used_shards_in_group", normalNodes.currentUsedShards().get());
                }
                builder.endObject();
            }
            {
                builder.startObject("frozen_nodes");
                builder.field("max_shards_in_cluster", frozenNodes.maxShardsInCluster());
                if (frozenNodes.currentUsedShards().isPresent()) {
                    builder.field("current_used_shards_in_group", frozenNodes.currentUsedShards().get());
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
