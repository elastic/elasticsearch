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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
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
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;

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
    private final Client client;

    private volatile TimeValue monitorInterval;
    private volatile boolean enabled;

    // Signals that all the prerequisites have been fulfilled and the monitoring task can be scheduled.
    private volatile boolean prerequisitesFulfilled;

    // Ensures that only one monitoring task will be in progress at any moment in time.
    // It removes the need to synchronize scheduling since at the event that there are two
    // monitoring tasks scheduled, one of them will be no-op.
    private final AtomicBoolean inProgress = new AtomicBoolean();

    // Keeps the latest health state that was successfully reported to the current health node.
    private final AtomicReference<DiskHealthInfo> lastReportedDiskHealthInfo = new AtomicReference<>();
    // Keeps the last seen health node. We use this variable to ensure that there wasn't a health node
    // change between the time we send an update until the time we update the lastReportedDiskHealthInfo.
    private final AtomicReference<String> lastSeenHealthNode = new AtomicReference<>();

    private LocalHealthMonitor(
        Settings settings,
        ClusterService clusterService,
        NodeService nodeService,
        ThreadPool threadPool,
        Client client
    ) {
        this.threadPool = threadPool;
        this.monitorInterval = POLL_INTERVAL_SETTING.get(settings);
        this.enabled = HealthNodeTaskExecutor.ENABLED_SETTING.get(settings);
        this.clusterService = clusterService;
        this.client = client;
        this.diskCheck = new DiskCheck(nodeService);
    }

    public static LocalHealthMonitor create(
        Settings settings,
        ClusterService clusterService,
        NodeService nodeService,
        ThreadPool threadPool,
        Client client
    ) {
        LocalHealthMonitor localHealthMonitor = new LocalHealthMonitor(settings, clusterService, nodeService, threadPool, client);
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
        maybeStartScheduleNow();
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
        maybeStartScheduleNow();
    }

    /**
     * This method schedules a monitoring tasks to be executed after the given delay and ensures that there will
     * be only a single motoring task in progress at any given time. We always check if the prerequisites are fulfilled
     * and if the health node is enabled before we schedule a monitoring task.
     */
    private void maybeStartSchedule(TimeValue time) {
        if (prerequisitesFulfilled && enabled) {
            threadPool.scheduleUnlessShuttingDown(
                time,
                ThreadPool.Names.MANAGEMENT,
                () -> ensureSingleRunAndReschedule(this::monitorHealth)
            );
        }
    }

    // Helper method that starts the monitoring without a delay.
    // Visible for testing
    void maybeStartScheduleNow() {
        maybeStartSchedule(TimeValue.ZERO);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentHealthNode = HealthNode.findHealthNode(event.state());
        resetOnHealthNodeChange(currentHealthNode, event);
        prerequisitesFulfilled = event.state().nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_5_0)
            && HealthMetadata.getFromClusterState(event.state()) != null
            && currentHealthNode != null;
        maybeStartScheduleNow();
    }

    private void resetOnHealthNodeChange(DiscoveryNode currentHealthNode, ClusterChangedEvent event) {
        if (isNewHealthNode(currentHealthNode, event)) {
            // The new health node might not have any information yet, so the last
            // reported health info gets reset to null.
            lastSeenHealthNode.set(currentHealthNode.getId());
            lastReportedDiskHealthInfo.set(null);
        }
    }

    // We compare the current health node against both the last seen health node from this node and the
    // health node reported in the previous cluster state to be safe that we do not miss any change due to
    // a flaky state.
    private boolean isNewHealthNode(DiscoveryNode currentHealthNode, ClusterChangedEvent event) {
        DiscoveryNode previousHealthNode = HealthNode.findHealthNode(event.previousState());
        if (currentHealthNode == null) {
            return true;
        }
        return Objects.equals(lastSeenHealthNode.get(), currentHealthNode.getId()) == false
            || Objects.equals(previousHealthNode, currentHealthNode) == false;
    }

    /**
     * This method evaluates the health info of this node and if there is a change it updates the health node
     * @param release, the runnable to always execute after the health node request was sent.
     * @return true, if the release steps were scheduled, false otherwise.
     */
    private boolean monitorHealth(Runnable release) {
        if (prerequisitesFulfilled == false) {
            return false;
        }
        ClusterState clusterState = clusterService.state();
        HealthMetadata healthMetadata = HealthMetadata.getFromClusterState(clusterState);
        if (healthMetadata == null) {
            logger.debug("Couldn't retrieve health metadata.");
            return false;
        }
        DiskHealthInfo previousHealth = this.lastReportedDiskHealthInfo.get();
        DiskHealthInfo currentHealth = diskCheck.getHealth(healthMetadata, clusterState);
        if (currentHealth.equals(previousHealth) == false) {
            String nodeId = clusterService.localNode().getId();
            String healthNodeId = lastSeenHealthNode.get();
            ActionListener<AcknowledgedResponse> listener = ActionListener.wrap(response -> {
                // Update the last reported value only if the health node hasn't changed.
                if (Objects.equals(healthNodeId, lastSeenHealthNode.get())
                    && lastReportedDiskHealthInfo.compareAndSet(previousHealth, currentHealth)) {
                    logger.debug(
                        "Health info [{}] successfully sent, last reported value: {}.",
                        currentHealth,
                        lastReportedDiskHealthInfo.get()
                    );
                }
            }, e -> logger.error(() -> format("Failed to send health info [%s] to health node, will try again.", currentHealth), e));
            client.execute(
                UpdateHealthInfoCacheAction.INSTANCE,
                new UpdateHealthInfoCacheAction.Request(nodeId, currentHealth),
                ActionListener.runAfter(listener, release)
            );
            return true;
        }
        return false;
    }

    /**
     * This method ensures that the monitoring given will not be executed in parallel at any given point in time and that it will
     * be rescheduled after it's finished. To enable an action to have async code, we provide the release steps as an argument.
     * We require that the action returns if the release code has been scheduled or not.
     */
    private void ensureSingleRunAndReschedule(Function<Runnable, Boolean> monitoringAction) {
        if (inProgress.compareAndSet(false, true)) {
            final Runnable release = new RunOnce(() -> {
                inProgress.set(false);
                // Scheduling happens after the flag inProgress is false, this ensures that
                // if the feature is enabled after the following schedule statement, the setEnabled
                // method will be able to schedule the next run, and it will not be a no-op.
                // We prefer to err towards an extra scheduling than miss the enabling of this feature alltogether.
                maybeStartSchedule(monitorInterval);
            });
            boolean released = false;
            try {
                released = monitoringAction.apply(release);
            } finally {
                if (released == false) {
                    release.run();
                }
            }
        }
    }

    @Nullable
    DiskHealthInfo getLastReportedDiskHealthInfo() {
        return lastReportedDiskHealthInfo.get();
    }

    // Visible for testing
    boolean isInProgress() {
        return inProgress.get();
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
