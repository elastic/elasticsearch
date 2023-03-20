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

        var normalNodesIndicator = calculateForNormalNodes(healthMetadata.getShardLimitsMetadata());
        var frozenNodesIndicator = calculateForFrozenNodes(healthMetadata.getShardLimitsMetadata());

        return mergeIndicators(normalNodesIndicator, frozenNodesIndicator);
    }

    private HealthIndicatorResult mergeIndicators(HealthIndicatorResult normalNodesIndicator, HealthIndicatorResult frozenNodesIndicator) {
        // TODO: implement
        return unknownIndicator();
    }

    private HealthIndicatorResult calculateForNormalNodes(HealthMetadata.ShardLimits shardLimits) {
        int maxShardsPerNodeSetting = shardLimits.maxShardsPerNode();
        var result = ShardLimitValidator.checkShardLimitForNormalNodes(maxShardsPerNodeSetting, 5, 1, clusterService.state());

        if (result.canAddShards() == false) {
            return createIndicator(
                HealthStatus.RED,
                "Cluster is close to reach the maximum number of shards (room available is lower than " + result.totalShardsToAdd() + ")",
                HealthIndicatorDetails.EMPTY,
                List.of(),
                List.of()
            );
        }

        result = ShardLimitValidator.checkShardLimitForNormalNodes(maxShardsPerNodeSetting, 10, 1, clusterService.state());

        if (result.canAddShards() == false) {
            return createIndicator(
                HealthStatus.YELLOW,
                "Cluster is close to reach the maximum number of shards (room available is lower than " + result.totalShardsToAdd() + ")",
                HealthIndicatorDetails.EMPTY,
                List.of(),
                List.of()
            );
        }

        return createIndicator(
            HealthStatus.GREEN,
            "The cluster has enough room to add new shards to normal nodes.",
            HealthIndicatorDetails.EMPTY,
            List.of(),
            List.of()
        );
    }

    private HealthIndicatorResult calculateForFrozenNodes(HealthMetadata.ShardLimits shardLimits) {
        int maxShardsPerNodeSetting = shardLimits.maxShardsPerNodeFrozen();
        var result = ShardLimitValidator.checkShardLimitForFrozenNodes(maxShardsPerNodeSetting, 5, 1, clusterService.state());

        if (result.canAddShards() == false) {
            return createIndicator(
                HealthStatus.RED,
                "Cluster is close to reach the maximum number of shards (room available is lower than " + result.totalShardsToAdd() + ")",
                HealthIndicatorDetails.EMPTY,
                List.of(),
                List.of()
            );
        }

        result = ShardLimitValidator.checkShardLimitForFrozenNodes(maxShardsPerNodeSetting, 10, 1, clusterService.state());

        if (result.canAddShards() == false) {
            return createIndicator(
                HealthStatus.YELLOW,
                "Cluster is close to reach the maximum number of shards (room available is lower than " + result.totalShardsToAdd() + ")",
                HealthIndicatorDetails.EMPTY,
                List.of(),
                List.of()
            );
        }

        return createIndicator(
            HealthStatus.GREEN,
            "The cluster has enough room to add new shards to frozen nodes.",
            HealthIndicatorDetails.EMPTY,
            List.of(),
            List.of()
        );
    }

    private HealthIndicatorResult unknownIndicator() {
        return createIndicator(HealthStatus.UNKNOWN, "No shard limits data.", HealthIndicatorDetails.EMPTY, List.of(), List.of());
    }
}
