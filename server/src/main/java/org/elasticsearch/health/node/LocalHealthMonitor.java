/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * This class monitors the health of the node regarding the load on difference resources.
 * Currently, it only checks for available disk space.
 */
public class LocalHealthMonitor implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(LocalHealthMonitor.class);

    public static final Setting<TimeValue> INTERVAL_SETTING = Setting.timeSetting(
        "health.reporting.local.monitor.interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final DiskCheck diskCheck;

    private volatile TimeValue monitorInterval;
    private volatile Scheduler.ScheduledCancellable scheduled;
    private volatile NodeHealth lastObservedHealth = NodeHealth.INITIALIZING;
    private volatile boolean enabled;

    public LocalHealthMonitor(Settings settings, ClusterService clusterService, NodeService nodeService, ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.monitorInterval = INTERVAL_SETTING.get(settings);
        this.enabled = HealthNodeTaskExecutor.ENABLED_SETTING.get(settings);
        this.clusterService = clusterService;
        this.diskCheck = new DiskCheck(nodeService);
        clusterService.addListener(this);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(INTERVAL_SETTING, this::setMonitorInterval);
        clusterSettings.addSettingsUpdateConsumer(HealthNodeTaskExecutor.ENABLED_SETTING, this::setEnabled);
    }

    void setMonitorInterval(TimeValue monitorInterval) {
        this.monitorInterval = monitorInterval;
        if (scheduled != null && scheduled.cancel()) {
            scheduleNextRunIfEnabled(new TimeValue(1));
        }
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (scheduled != null) {
            scheduled.cancel();
            scheduleNextRunIfEnabled(new TimeValue(1));
        }
    }

    private void scheduleNextRunIfEnabled(TimeValue time) {
        if (enabled && threadPool.scheduler().isShutdown() == false) {
            scheduled = threadPool.schedule(this::monitorHealth, time, ThreadPool.Names.MANAGEMENT);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // Wait until every node in the cluster is upgraded to 8.4.0 or later
        if (event.state().nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_4_0)) {
            // Wait until the health metadata is available in the cluster state
            if (HealthMetadata.getHealthCustomMetadata(event.state()) != null) {
                scheduleNextRunIfEnabled(new TimeValue(1));
                // We do not track the changes in the cluster state anymore, if the health metadata is changed,
                // the updated metadata will be used in the next round
                clusterService.removeListener(this);
            }
        }
    }

    void monitorHealth() {
        ClusterState clusterState = clusterService.state();
        HealthMetadata healthMetadata = HealthMetadata.getHealthCustomMetadata(clusterState);
        assert healthMetadata != null : "health metadata should have been initialized.";
        NodeHealth previousHealth = this.lastObservedHealth;
        NodeHealth currentHealth = new NodeHealth(diskCheck.getHealth(healthMetadata, clusterState));
        if (currentHealth.equals(previousHealth) == false) {
            this.lastObservedHealth = currentHealth;
            logger.info("Node health changed from {} to {}", previousHealth, currentHealth);
        }
        scheduleNextRunIfEnabled(monitorInterval);
    }

    NodeHealth getLastObservedHealth() {
        return lastObservedHealth;
    }

    /**
     * Determines the disk health of this node by checking if it exceeds the thresholds defined in the health metadata.
     */
    static class DiskCheck {
        private final NodeService nodeService;

        DiskCheck(NodeService nodeService) {
            this.nodeService = nodeService;
        }

        NodeHealth.Disk getHealth(HealthMetadata healthMetadata, ClusterState clusterState) {
            DiscoveryNode node = clusterState.getNodes().getLocalNode();
            HealthMetadata.Disk diskMetadata = healthMetadata.getDiskMetadata();
            DiskUsage usage = getDiskUsage();
            if (usage == null) {
                return new NodeHealth.Disk(HealthStatus.UNKNOWN, NodeHealth.Disk.Cause.NODE_HAS_NO_DISK_STATS);
            }

            ByteSizeValue totalBytes = ByteSizeValue.ofBytes(usage.getTotalBytes());

            if (node.isDedicatedFrozenNode()) {
                long frozenFloodStageThreshold = diskMetadata.getFreeBytesFrozenFloodStageWatermark(totalBytes).getBytes();
                if (usage.getFreeBytes() < frozenFloodStageThreshold) {
                    logger.debug("flood stage disk watermark [{}] exceeded on {}", frozenFloodStageThreshold, usage);
                    return new NodeHealth.Disk(HealthStatus.RED, NodeHealth.Disk.Cause.FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD);
                }
                return new NodeHealth.Disk(HealthStatus.GREEN);
            }

            long floodStageThreshold = diskMetadata.getFreeBytesFloodStageWatermark(totalBytes).getBytes();
            if (usage.getFreeBytes() < floodStageThreshold) {
                return new NodeHealth.Disk(HealthStatus.RED, NodeHealth.Disk.Cause.NODE_OVER_THE_FLOOD_STAGE_THRESHOLD);
            }

            long highThreshold = diskMetadata.getFreeBytesHighWatermark(totalBytes).getBytes();
            if (usage.getFreeBytes() < highThreshold && hasRelocatingShards(clusterState, node.getId()) == false) {
                return new NodeHealth.Disk(HealthStatus.YELLOW, NodeHealth.Disk.Cause.NODE_OVER_HIGH_THRESHOLD);
            }
            return new NodeHealth.Disk(HealthStatus.GREEN);
        }

        private DiskUsage getDiskUsage() {
            NodeStats nodeStats = nodeService.stats(
                CommonStatsFlags.NONE,
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
                false
            );
            final String nodeId = nodeStats.getNode().getId();
            final String nodeName = nodeStats.getNode().getName();
            if (nodeStats.getFs() == null) {
                logger.debug("node [{}/{}] did not return any filesystem stats", nodeName, nodeId);
                return null;
            }

            FsInfo.Path leastAvailablePath = null;
            for (FsInfo.Path info : nodeStats.getFs()) {
                if (leastAvailablePath == null) {
                    leastAvailablePath = info;
                } else if (leastAvailablePath.getAvailable().getBytes() > info.getAvailable().getBytes()) {
                    leastAvailablePath = info;
                }
            }
            if (leastAvailablePath == null) {
                logger.debug("node [{}/{}] did not return any filesystem stats", nodeName, nodeId);
                return null;
            }
            if (leastAvailablePath.getTotal().getBytes() < 0) {
                logger.debug("node [{}/{}] reported negative total disk space", nodeName, nodeId);
                return null;
            }

            return new DiskUsage(
                nodeId,
                nodeName,
                leastAvailablePath.getPath(),
                leastAvailablePath.getTotal().getBytes(),
                leastAvailablePath.getAvailable().getBytes()
            );
        }

        private boolean hasRelocatingShards(ClusterState clusterState, String nodeId) {
            return clusterState.getRoutingNodes().node(nodeId).shardsWithState(ShardRoutingState.RELOCATING).isEmpty() == false;
        }
    }

    /**
     * The health of this node which consists by the health status of different resources, currently only disk space.
     */
    record NodeHealth(Disk disk) {
        static final NodeHealth INITIALIZING = new NodeHealth(new Disk(HealthStatus.UNKNOWN, Disk.Cause.NODE_JUST_STARTED));

        /**
         * The health status of the disk space of this node along with the cause.
         */
        record Disk(HealthStatus healthStatus, Cause cause) {
            Disk(HealthStatus healthStatus) {
                this(healthStatus, null);
            }

            enum Cause {
                NODE_OVER_HIGH_THRESHOLD,
                NODE_OVER_THE_FLOOD_STAGE_THRESHOLD,
                FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD,
                NODE_HAS_NO_DISK_STATS,
                NODE_JUST_STARTED
            }
        }
    }
}
