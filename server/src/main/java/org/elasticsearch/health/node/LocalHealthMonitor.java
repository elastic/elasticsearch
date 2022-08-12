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
import org.elasticsearch.node.NodeService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class monitors the health of the node regarding the load on several resources.
 * Currently, it only checks for available disk space. Furthermore, it informs the health
 * node about the local health upon change or when a new node is detected.
 */
public class LocalHealthMonitor implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(LocalHealthMonitor.class);

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
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
    private volatile boolean enabled;
    // Signals that all the prerequisites have been fulfilled and the monitoring task can be scheduled.
    private volatile boolean prerequisitesFulfilled;
    // Ensures that only one monitoring task will be in progress at any moment in time.
    // It removes the need to synchronize scheduling since at the event that there are two
    // monitoring tasks scheduled, one of them will be no-op.
    private final AtomicBoolean inProgress = new AtomicBoolean();
    // Keeps the latest health state that was successfully reported.
    private volatile DiskHealthInfo lastReportedDiskHealthInfo = null;

    private LocalHealthMonitor(Settings settings, ClusterService clusterService, NodeService nodeService, ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.monitorInterval = POLL_INTERVAL_SETTING.get(settings);
        this.enabled = HealthNodeTaskExecutor.ENABLED_SETTING.get(settings);
        this.clusterService = clusterService;
        this.diskCheck = new DiskCheck(nodeService);
    }

    public static LocalHealthMonitor create(
        Settings settings,
        ClusterService clusterService,
        NodeService nodeService,
        ThreadPool threadPool
    ) {
        LocalHealthMonitor localHealthMonitor = new LocalHealthMonitor(settings, clusterService, nodeService, threadPool);
        localHealthMonitor.registerListeners();
        return localHealthMonitor;
    }

    private void registerListeners() {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::setMonitorInterval);
        clusterSettings.addSettingsUpdateConsumer(HealthNodeTaskExecutor.ENABLED_SETTING, this::setEnabled);
        clusterService.addListener(this);
    }

    void setMonitorInterval(TimeValue monitorInterval) {
        this.monitorInterval = monitorInterval;
        maybeScheduleNow();
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
        maybeScheduleNow();
    }

    /**
     * We always check if the prerequisites are fulfilled and if the health node
     * is enabled before we schedule a monitoring task.
     */
    private void maybeScheduleNextRun(TimeValue time) {
        if (prerequisitesFulfilled && enabled) {
            threadPool.scheduleUnlessShuttingDown(time, ThreadPool.Names.MANAGEMENT, this::monitorHealth);
        }
    }

    // Helper method that starts the monitoring without a delay.
    private void maybeScheduleNow() {
        maybeScheduleNextRun(TimeValue.ZERO);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (prerequisitesFulfilled == false) {
            prerequisitesFulfilled = event.state().nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_5_0)
                && HealthMetadata.getFromClusterState(event.state()) != null;
            maybeScheduleNow();
        }
    }

    // Visible for testing
    void monitorHealth() {
        if (inProgress.compareAndSet(false, true)) {
            ClusterState clusterState = clusterService.state();
            HealthMetadata healthMetadata = HealthMetadata.getFromClusterState(clusterState);
            assert healthMetadata != null : "health metadata should have been initialized.";
            DiskHealthInfo previousHealth = this.lastReportedDiskHealthInfo;
            DiskHealthInfo currentHealth = diskCheck.getHealth(healthMetadata, clusterState);
            if (currentHealth.equals(previousHealth) == false) {
                logger.debug("Health status changed from {} to {}", previousHealth, currentHealth);
                this.lastReportedDiskHealthInfo = currentHealth;
            }
            inProgress.set(false);
            // Scheduling happens after the flag inProgress is false, this ensures that
            // if the feature is enabled after the following schedule statement, the setEnabled
            // method will be able to schedule the next run, and it will not be a no-op.
            // We prefer to err towards an extra scheduling than miss the enabling of this feature alltogether.
            maybeScheduleNextRun(monitorInterval);
        }
    }

    DiskHealthInfo getLastReportedDiskHealthInfo() {
        return lastReportedDiskHealthInfo;
    }

    /**
     * Determines the disk health of this node by checking if it exceeds the thresholds defined in the health metadata.
     */
    static class DiskCheck {
        private final NodeService nodeService;

        DiskCheck(NodeService nodeService) {
            this.nodeService = nodeService;
        }

        DiskHealthInfo getHealth(HealthMetadata healthMetadata, ClusterState clusterState) {
            DiscoveryNode node = clusterState.getNodes().getLocalNode();
            HealthMetadata.Disk diskMetadata = healthMetadata.getDiskMetadata();
            DiskUsage usage = getDiskUsage();
            if (usage == null) {
                return new DiskHealthInfo(HealthStatus.UNKNOWN, DiskHealthInfo.Cause.NODE_HAS_NO_DISK_STATS);
            }

            ByteSizeValue totalBytes = ByteSizeValue.ofBytes(usage.getTotalBytes());

            if (node.isDedicatedFrozenNode()) {
                long frozenFloodStageThreshold = diskMetadata.getFreeBytesFrozenFloodStageWatermark(totalBytes).getBytes();
                if (usage.getFreeBytes() < frozenFloodStageThreshold) {
                    logger.debug("flood stage disk watermark [{}] exceeded on {}", frozenFloodStageThreshold, usage);
                    return new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD);
                }
                return new DiskHealthInfo(HealthStatus.GREEN);
            }

            long floodStageThreshold = diskMetadata.getFreeBytesFloodStageWatermark(totalBytes).getBytes();
            if (usage.getFreeBytes() < floodStageThreshold) {
                return new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.NODE_OVER_THE_FLOOD_STAGE_THRESHOLD);
            }

            long highThreshold = diskMetadata.getFreeBytesHighWatermark(totalBytes).getBytes();
            if (usage.getFreeBytes() < highThreshold && hasRelocatingShards(clusterState, node.getId()) == false) {
                return new DiskHealthInfo(HealthStatus.YELLOW, DiskHealthInfo.Cause.NODE_OVER_HIGH_THRESHOLD);
            }
            return new DiskHealthInfo(HealthStatus.GREEN);
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
            return DiskUsage.findLeastAvailablePath(nodeStats);
        }

        private boolean hasRelocatingShards(ClusterState clusterState, String nodeId) {
            return clusterState.getRoutingNodes().node(nodeId).shardsWithState(ShardRoutingState.RELOCATING).isEmpty() == false;
        }
    }
}
