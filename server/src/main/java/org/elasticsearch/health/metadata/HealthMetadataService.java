/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.metadata;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.elasticsearch.health.node.selection.HealthNodeTaskExecutor.ENABLED_SETTING;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN;

/**
 * Keeps the health metadata in the cluster state up to date. It listens to master elections and changes in the disk thresholds.
 */
public class HealthMetadataService {

    private static final Logger logger = LogManager.getLogger(HealthMetadataService.class);

    private final ClusterService clusterService;
    private final ClusterStateListener clusterStateListener;
    private final MasterServiceTaskQueue<UpsertHealthMetadataTask> taskQueue;
    private volatile boolean enabled;

    // Signifies that a node has been elected as master, but it was not able yet to publish its health metadata for
    // other reasons for example not all nodes of the cluster are 8.4.0 or newer
    private volatile boolean readyToPublish = false;
    // Allows us to know if this node is the elected master without checking the cluster state, effectively protecting
    // us from checking the cluster state before the cluster state is initialized
    private volatile boolean isMaster = false;

    private HealthMetadataService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.clusterStateListener = this::updateOnClusterStateChange;
        this.enabled = clusterService.getClusterSettings().get(ENABLED_SETTING);
        this.taskQueue = clusterService.createTaskQueue(
            "health metadata service",
            Priority.NORMAL,
            new UpsertHealthMetadataTask.Executor()
        );
    }

    public static HealthMetadataService create(ClusterService clusterService) {
        HealthMetadataService healthMetadataService = new HealthMetadataService(clusterService);
        healthMetadataService.registerListeners();
        return healthMetadataService;
    }

    private void registerListeners() {
        if (this.enabled) {
            this.clusterService.addListener(clusterStateListener);
        }

        var clusterSettings = clusterService.getClusterSettings();
        Stream.of(
            CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING
        )
            .forEach(
                setting -> clusterSettings.addSettingsUpdateConsumer(
                    setting,
                    value -> updateOnSettingsUpdated(new UpdateDiskMetadata(setting.getKey(), value.getStringRep()))
                )
            );

        Stream.of(
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING,
            CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING
        )
            .forEach(
                setting -> clusterSettings.addSettingsUpdateConsumer(
                    setting,
                    value -> updateOnSettingsUpdated(new UpdateDiskMetadata(setting.getKey(), value.getStringRep()))
                )
            );

        Stream.of(SETTING_CLUSTER_MAX_SHARDS_PER_NODE, SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN)
            .forEach(
                setting -> clusterSettings.addSettingsUpdateConsumer(
                    setting,
                    value -> updateOnSettingsUpdated(new UpdateShardLimitsMetadata(setting.getKey(), value))
                )
            );

        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENABLED_SETTING, this::enable);
    }

    private void enable(boolean enabled) {
        this.enabled = enabled;
        if (this.enabled) {
            clusterService.addListener(clusterStateListener);
            resetHealthMetadata("health-node-enabled");
        } else {
            clusterService.removeListener(clusterStateListener);
            readyToPublish = false;
        }
    }

    private void updateOnClusterStateChange(ClusterChangedEvent event) {
        final boolean wasMaster = event.previousState().nodes().isLocalNodeElectedMaster();
        isMaster = event.localNodeMaster();
        if (isMaster && wasMaster == false) {
            readyToPublish = true;
        } else if (isMaster == false) {
            readyToPublish = false;
        }
        // Wait until every node in the cluster is upgraded to 8.5.0 or later
        if (event.state().nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_5_0)) {
            if (readyToPublish) {
                resetHealthMetadata("health-metadata-update-master-election");
                readyToPublish = false;
            }
        }
    }

    private void updateOnSettingsUpdated(UpsertHealthMetadataTask task) {
        // We do not use the cluster state to check if this is the master node because the cluster state might not have been initialized
        if (isMaster && enabled) {
            ClusterState clusterState = clusterService.state();
            if (clusterState.nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_5_0)) {
                taskQueue.submitTask("health-metadata-update", task, null);
            }
        }
    }

    private void resetHealthMetadata(String source) {
        taskQueue.submitTask(source, new InsertHealthMetadata(clusterService.getClusterSettings()), null);
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(ClusterState.Custom.class, HealthMetadata.TYPE, HealthMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, HealthMetadata.TYPE, HealthMetadata::readDiffFrom)
        );
    }

    /**
     * A base class for health metadata cluster state update tasks.
     */
    abstract static class UpsertHealthMetadataTask implements ClusterStateTaskListener {

        @Override
        public void onFailure(@Nullable Exception e) {
            logger.log(
                MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.WARN,
                () -> "failure during health metadata update",
                e
            );
        }

        ClusterState execute(ClusterState currentState) {
            var initialHealthMetadata = HealthMetadata.getFromClusterState(currentState);
            var finalHealthMetadata = doExecute(initialHealthMetadata);
            return finalHealthMetadata.equals(initialHealthMetadata)
                ? currentState
                : currentState.copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, finalHealthMetadata));
        }

        abstract HealthMetadata doExecute(HealthMetadata initialHealthMetadata);

        static class Executor extends SimpleBatchedExecutor<UpsertHealthMetadataTask, Void> {

            @Override
            public Tuple<ClusterState, Void> executeTask(UpsertHealthMetadataTask task, ClusterState clusterState) {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(UpsertHealthMetadataTask task, Void unused) {}
        }
    }

    /**
     * A health metadata cluster state update task that updates a single setting of disk with the new value.
     */
    static class UpdateDiskMetadata extends UpsertHealthMetadataTask {
        private final String setting;
        private final String value;

        UpdateDiskMetadata(String setting, String value) {
            this.setting = setting;
            this.value = value;
        }

        @Override
        HealthMetadata doExecute(HealthMetadata initialHealthMetadata) {
            assert initialHealthMetadata != null : "health metadata should have been initialized";
            return new HealthMetadata(
                diskMetadataFrom(initialHealthMetadata.getDiskMetadata()),
                initialHealthMetadata.getShardLimitsMetadata()
            );
        }

        private HealthMetadata.Disk diskMetadataFrom(HealthMetadata.Disk initialDiskMetadata) {
            HealthMetadata.Disk.Builder builder = HealthMetadata.Disk.newBuilder(initialDiskMetadata);
            if (CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey().equals(setting)) {
                builder.highWatermark(value, setting);
            }
            if (CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey().equals(setting)) {
                builder.floodStageWatermark(value, setting);
            }
            if (CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey().equals(setting)) {
                builder.frozenFloodStageWatermark(value, setting);
            }
            if (CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey().equals(setting)) {
                builder.frozenFloodStageMaxHeadroom(value, setting);
            }
            if (CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey().equals(setting)) {
                builder.highMaxHeadroom(value, setting);
            }
            if (CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey().equals(setting)) {
                builder.floodStageMaxHeadroom(value, setting);
            }
            return builder.build();
        }
    }

    /**
     * A health metadata cluster state update task that updates a single setting of shardLimits with the new value.
     */
    static class UpdateShardLimitsMetadata extends UpsertHealthMetadataTask {

        private final String setting;
        private final Integer value;

        UpdateShardLimitsMetadata(String setting, Integer value) {
            this.setting = setting;
            this.value = value;
        }

        @Override
        HealthMetadata doExecute(HealthMetadata initialHealthMetadata) {
            assert initialHealthMetadata != null : "health metadata should have been initialized";
            return new HealthMetadata(
                initialHealthMetadata.getDiskMetadata(),
                shardLimitsMetadataFrom(initialHealthMetadata.getShardLimitsMetadata())
            );
        }

        private HealthMetadata.ShardLimits shardLimitsMetadataFrom(HealthMetadata.ShardLimits initialShardLimits) {
            var builder = HealthMetadata.ShardLimits.newBuilder(initialShardLimits);
            if (SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey().equals(setting)) {
                builder.maxShardsPerNode(value);
            }
            if (SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey().equals(setting)) {
                builder.maxShardsPerNodeFrozen(value);
            }
            return builder.build();
        }
    }

    /**
     * A health metadata cluster state update task that reads the settings from the local node and resets the
     * health metadata in the cluster state with these values.
     */
    static class InsertHealthMetadata extends UpsertHealthMetadataTask {

        private final ClusterSettings settings;

        InsertHealthMetadata(ClusterSettings settings) {
            this.settings = settings;
        }

        @Override
        HealthMetadata doExecute(HealthMetadata initialHealthMetadata) {
            return new HealthMetadata(
                new HealthMetadata.Disk(
                    settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING),
                    settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING),
                    settings.get(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING),
                    settings.get(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING),
                    settings.get(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING),
                    settings.get(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING)
                ),
                new HealthMetadata.ShardLimits(
                    settings.get(SETTING_CLUSTER_MAX_SHARDS_PER_NODE),
                    settings.get(SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN)
                )
            );
        }
    }
}
