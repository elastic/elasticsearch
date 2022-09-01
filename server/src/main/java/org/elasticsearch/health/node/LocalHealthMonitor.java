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
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.health.node.action.HealthNodeNotDiscoveredException;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeNotConnectedException;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;

/**
 * This class monitors the health of the node regarding the load on several resources.
 * Currently, it only checks for available disk space. Furthermore, it informs the health
 * node about the local health upon change or when a new node is detected or when the
 * master node changed.
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

    // Keeps the latest health state that was successfully reported to the current health node.
    private final AtomicReference<DiskHealthInfo> lastReportedDiskHealthInfo = new AtomicReference<>();
    // Keeps the last seen health node. We use this variable to ensure that there wasn't a health node
    // change between the time we send an update until the time we update the lastReportedDiskHealthInfo.
    private final AtomicReference<String> lastSeenHealthNode = new AtomicReference<>();
    // Using a volatile reference to ensure that there is a single monitoring task running at all times
    // without extra synchronization is sufficient because all the writes are executed on the cluster
    // applier thread.
    private volatile Scheduler.Cancellable scheduledMonitoringTask;

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

    // When the monitoring interval changed we reschedule the health monitoring task with the new interval.
    void setMonitorInterval(TimeValue monitorInterval) {
        this.monitorInterval = monitorInterval;
        cancelMonitoringTask();
        scheduleMonitoringTaskIfNecessary();
    }

    // When the health node is enabled we try to schedule a monitoring task if it is not
    // already scheduled, no need to reschedule if it is already there since there was
    // no configuration change. When the health node is disabled we cancel the schedule.
    void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            scheduleMonitoringTaskIfNecessary();
        } else {
            cancelMonitoringTask();
        }
    }

    private void cancelMonitoringTask() {
        // If there is an existing schedule, cancel it
        Scheduler.Cancellable existingTask = scheduledMonitoringTask;
        if (existingTask != null) {
            existingTask.cancel();
        }
    }

    private void scheduleMonitoringTaskIfNecessary() {
        if (prerequisitesFulfilled && enabled) {
            if (isScheduled() == false) {
                scheduledMonitoringTask = new MonitoringTask(
                    monitorInterval,
                    ThreadPool.Names.MANAGEMENT,
                    threadPool,
                    lastReportedDiskHealthInfo,
                    lastSeenHealthNode,
                    diskCheck,
                    clusterService,
                    client
                );
                logger.debug("Monitoring task started {}", scheduledMonitoringTask);
            } else {
                logger.debug("Monitoring task already started {}, skipping", scheduledMonitoringTask);
            }
        }
    }

    private boolean isScheduled() {
        Scheduler.Cancellable scheduled = this.scheduledMonitoringTask;
        return scheduled != null && scheduled.isCancelled() == false;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentHealthNode = HealthNode.findHealthNode(event.state());
        DiscoveryNode currentMasterNode = event.state().nodes().getMasterNode();
        boolean healthNodeChanged = hasHealthNodeChanged(currentHealthNode, event);
        boolean masterNodeChanged = hasMasterNodeChanged(currentMasterNode, event);
        if (healthNodeChanged || masterNodeChanged) {
            // On health node or on master node changes, the health node might be reset so the reported
            // health info gets reset to null, to ensure it will be resent.
            lastSeenHealthNode.set(currentHealthNode == null ? null : currentHealthNode.getId());
            lastReportedDiskHealthInfo.set(null);
            logger.debug(
                "Change detected, resetting the health monitoring [masterNodeChanged={},healthNodeChanged={},newHealthNode={}",
                masterNodeChanged,
                healthNodeChanged,
                lastSeenHealthNode.get()
            );
        }
        prerequisitesFulfilled = event.state().nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_5_0)
            && HealthMetadata.getFromClusterState(event.state()) != null
            && currentHealthNode != null
            && currentMasterNode != null;
        if (prerequisitesFulfilled == false || healthNodeChanged || masterNodeChanged) {
            cancelMonitoringTask();
        }
        if (prerequisitesFulfilled) {
            scheduleMonitoringTaskIfNecessary();
        }
    }

    private boolean hasMasterNodeChanged(DiscoveryNode currentMasterNode, ClusterChangedEvent event) {
        DiscoveryNode previousMasterNode = event.previousState().nodes().getMasterNode();
        if (currentMasterNode == null || previousMasterNode == null) {
            return currentMasterNode != previousMasterNode;
        }
        return previousMasterNode.getEphemeralId().equals(currentMasterNode.getEphemeralId()) == false;
    }

    // We compare the current health node against both the last seen health node from this node and the
    // health node reported in the previous cluster state to be safe that we do not miss any change due to
    // a flaky state.
    private boolean hasHealthNodeChanged(DiscoveryNode currentHealthNode, ClusterChangedEvent event) {
        DiscoveryNode previousHealthNode = HealthNode.findHealthNode(event.previousState());
        return Objects.equals(lastSeenHealthNode.get(), currentHealthNode == null ? null : currentHealthNode.getId()) == false
            || Objects.equals(previousHealthNode, currentHealthNode) == false;
    }

    @Nullable
    DiskHealthInfo getLastReportedDiskHealthInfo() {
        return lastReportedDiskHealthInfo.get();
    }

    /**
     * This class is responsible for running the health monitoring in the configured intervals. The first execution happens
     * upon initialization. If there is an exception, it will log it and continue to schedule the next execution unless it
     * was a EsRejectedExecutionException which will cancel the task.
     */
    static class MonitoringTask implements Runnable, Scheduler.Cancellable {

        private final TimeValue interval;
        private final String executor;
        private final Scheduler scheduler;
        private final ClusterService clusterService;
        private final DiskCheck diskCheck;
        private final Client client;

        private final AtomicReference<DiskHealthInfo> lastReportedDiskHealthInfo;
        private final AtomicReference<String> lastSeenHealthNode;

        private volatile boolean cancelled = false;

        MonitoringTask(
            TimeValue interval,
            String executor,
            Scheduler scheduler,
            AtomicReference<DiskHealthInfo> lastReportedDiskHealthInfo,
            AtomicReference<String> lastSeenHealthNode,
            DiskCheck diskCheck,
            ClusterService clusterService,
            Client client
        ) {
            this.interval = interval;
            this.executor = executor;
            this.scheduler = scheduler;
            this.lastReportedDiskHealthInfo = lastReportedDiskHealthInfo;
            this.lastSeenHealthNode = lastSeenHealthNode;
            this.clusterService = clusterService;
            this.diskCheck = diskCheck;
            this.client = client;
            scheduler.schedule(this, TimeValue.ZERO, executor);
        }

        @Override
        public boolean cancel() {
            final boolean result = cancelled;
            cancelled = true;
            return result == false;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        /**
         * This method evaluates the health info of this node and if there is a change it sends an update request to the health node
         */
        @Override
        public void run() {
            if (cancelled) {
                return;
            }
            boolean nextRunScheduled = false;
            Runnable runOnceScheduleNextRunIfNecessary = new RunOnce(this::scheduleNextRunIfNecessary);
            try {
                ClusterState clusterState = clusterService.state();
                HealthMetadata healthMetadata = HealthMetadata.getFromClusterState(clusterState);
                if (healthMetadata != null) {
                    DiskHealthInfo previousHealth = this.lastReportedDiskHealthInfo.get();
                    DiskHealthInfo currentHealth = diskCheck.getHealth(healthMetadata, clusterState);
                    if (currentHealth.equals(previousHealth) == false) {
                        String nodeId = clusterService.localNode().getId();
                        String healthNodeId = lastSeenHealthNode.get();
                        ActionListener<AcknowledgedResponse> listener = ActionListener.wrap(response -> {
                            // Update the last reported value only if the health node hasn't changed.
                            if (Objects.equals(healthNodeId, lastSeenHealthNode.get())
                                && lastReportedDiskHealthInfo.compareAndSet(previousHealth, currentHealth)) {
                                logger.info(
                                    "Health info [{}] successfully sent, last reported value: {}.",
                                    currentHealth,
                                    lastReportedDiskHealthInfo.get()
                                );
                            }
                        }, e -> {
                            if (e.getCause() instanceof NodeNotConnectedException
                                || e.getCause() instanceof HealthNodeNotDiscoveredException) {
                                logger.warn("Failed to connect to the health node [{}], will try again.", e.getCause().getMessage());
                            } else {
                                logger.error(
                                    () -> format("Failed to send health info [%s] to health node, will try again.", currentHealth),
                                    e
                                );
                            }
                        });
                        client.execute(
                            UpdateHealthInfoCacheAction.INSTANCE,
                            new UpdateHealthInfoCacheAction.Request(nodeId, currentHealth),
                            ActionListener.runAfter(listener, runOnceScheduleNextRunIfNecessary)
                        );
                        nextRunScheduled = true;
                    }
                }
            } catch (Exception e) {
                logger.warn(() -> format("failed to run scheduled health monitoring task on thread pool [%s]", executor), e);
            } finally {
                if (nextRunScheduled == false) {
                    runOnceScheduleNextRunIfNecessary.run();
                }
            }
        }

        private void scheduleNextRunIfNecessary() {
            if (cancelled) {
                return;
            }
            try {
                scheduler.schedule(this, interval, executor);
            } catch (final EsRejectedExecutionException e) {
                cancelled = true;
                if (logger.isDebugEnabled()) {
                    logger.debug(() -> format("scheduled health monitoring task was rejected on thread pool [%s]", executor), e);
                }
            }
        }

        @Override
        public String toString() {
            return "HealthMonitoringTask{interval=" + interval + ", cancelled=" + cancelled + "}";
        }
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
