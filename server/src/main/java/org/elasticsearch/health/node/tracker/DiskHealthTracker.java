/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.health.node.DiskHealthInfo;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.node.NodeService;

import java.util.Set;

/**
 * Determines the disk health of this node by checking if it exceeds the thresholds defined in the health metadata.
 */
public class DiskHealthTracker extends HealthTracker<DiskHealthInfo> {
    private static final Logger logger = LogManager.getLogger(DiskHealthTracker.class);

    private final NodeService nodeService;
    private final ClusterService clusterService;

    public DiskHealthTracker(NodeService nodeService, ClusterService clusterService) {
        this.nodeService = nodeService;
        this.clusterService = clusterService;
    }

    /**
     * Determines the disk health of this node by checking if it exceeds the thresholds defined in the health metadata.
     *
     * @return the current disk health info.
     */
    @Override
    protected DiskHealthInfo determineCurrentHealth() {
        var clusterState = clusterService.state();
        var healthMetadata = HealthMetadata.getFromClusterState(clusterState);
        DiscoveryNode node = clusterState.getNodes().getLocalNode();
        HealthMetadata.Disk diskMetadata = healthMetadata.getDiskMetadata();
        DiskUsage usage = getDiskUsage();
        if (usage == null) {
            return new DiskHealthInfo(HealthStatus.UNKNOWN, DiskHealthInfo.Cause.NODE_HAS_NO_DISK_STATS);
        }

        ByteSizeValue totalBytes = ByteSizeValue.ofBytes(usage.totalBytes());

        if (node.isDedicatedFrozenNode() || isDedicatedSearchNode(node)) {
            long frozenFloodStageThreshold = diskMetadata.getFreeBytesFrozenFloodStageWatermark(totalBytes).getBytes();
            if (usage.freeBytes() < frozenFloodStageThreshold) {
                logger.debug("Flood stage disk watermark [{}] exceeded on {}", frozenFloodStageThreshold, usage);
                return new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD);
            }
            return new DiskHealthInfo(HealthStatus.GREEN);
        }
        long floodStageThreshold = diskMetadata.getFreeBytesFloodStageWatermark(totalBytes).getBytes();
        if (usage.freeBytes() < floodStageThreshold) {
            logger.debug("Flood stage disk watermark [{}] exceeded on {}", floodStageThreshold, usage);
            return new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.NODE_OVER_THE_FLOOD_STAGE_THRESHOLD);
        }

        long highThreshold = diskMetadata.getFreeBytesHighWatermark(totalBytes).getBytes();
        if (usage.freeBytes() < highThreshold) {
            if (node.canContainData()) {
                // for data nodes only report YELLOW if shards can't move away from the node
                if (DiskHealthTracker.hasRelocatingShards(clusterState, node) == false) {
                    logger.debug("High disk watermark [{}] exceeded on {}", highThreshold, usage);
                    return new DiskHealthInfo(HealthStatus.YELLOW, DiskHealthInfo.Cause.NODE_OVER_HIGH_THRESHOLD);
                }
            } else {
                // for non-data nodes report YELLOW when the disk high watermark is breached
                logger.debug("High disk watermark [{}] exceeded on {}", highThreshold, usage);
                return new DiskHealthInfo(HealthStatus.YELLOW, DiskHealthInfo.Cause.NODE_OVER_HIGH_THRESHOLD);
            }
        }
        return new DiskHealthInfo(HealthStatus.GREEN);
    }

    @Override
    protected void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, DiskHealthInfo healthInfo) {
        builder.diskHealthInfo(healthInfo);
    }

    private static boolean isDedicatedSearchNode(DiscoveryNode node) {
        Set<DiscoveryNodeRole> roles = node.getRoles();
        return roles.contains(DiscoveryNodeRole.SEARCH_ROLE)
            && roles.stream().filter(DiscoveryNodeRole::canContainData).anyMatch(r -> r != DiscoveryNodeRole.SEARCH_ROLE) == false;
    }

    private DiskUsage getDiskUsage() {
        NodeStats nodeStats = nodeService.stats(
            CommonStatsFlags.NONE,
            false,
            false,
            false,
            false,
            false,
            true,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false
        );
        return DiskUsage.findLeastAvailablePath(nodeStats);
    }

    static boolean hasRelocatingShards(ClusterState clusterState, DiscoveryNode node) {
        RoutingNode routingNode = clusterState.getRoutingNodes().node(node.getId());
        if (routingNode == null) {
            // routing node will be null for non-data nodes
            return false;
        }
        return routingNode.numberOfShardsWithState(ShardRoutingState.RELOCATING) > 0;
    }
}
