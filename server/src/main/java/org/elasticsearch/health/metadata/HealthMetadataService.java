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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.GatewayService;

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
    private volatile HealthMetadata previousHealthMetadata;
    private volatile HealthMetadata healthMetadata;

    private HealthMetadataService(ClusterService clusterService, Settings settings) {
        this.clusterService = clusterService;
        this.clusterStateListener = this::updateOnClusterStateChange;
        this.enabled = ENABLED_SETTING.get(settings);
        this.previousHealthMetadata = initialHealthMetadata(settings);
        this.healthMetadata = this.previousHealthMetadata;
        this.taskQueue = clusterService.createTaskQueue(
            "health metadata service",
            Priority.NORMAL,
            new UpsertHealthMetadataTask.Executor()
        );
    }

    public static HealthMetadataService create(ClusterService clusterService, Settings settings) {
        HealthMetadataService healthMetadataService = new HealthMetadataService(clusterService, settings);
        healthMetadataService.registerListeners();
        return healthMetadataService;
    }
    private void registerListeners() {
        if (this.enabled) {
            this.clusterService.addListener(clusterStateListener);
        }

        var clusterSettings = clusterService.getClusterSettings();

        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::enable);

        Stream.of(
            CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING
        )
            .forEach(
                setting -> clusterSettings.addSettingsUpdateConsumer(
                    setting,
                    value -> updateOnDiskSettingsUpdated(setting.getKey(), value.getStringRep())
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
                    value -> updateOnDiskSettingsUpdated(setting.getKey(), value.getStringRep())
                )
            );

        Stream.of(SETTING_CLUSTER_MAX_SHARDS_PER_NODE, SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN)
            .forEach(
                setting -> clusterSettings.addSettingsUpdateConsumer(
                    setting,
                    value -> updateOnShardLimitsSettingsUpdated(setting.getKey(), value)
                )
            );
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
            } else if (isMaster && previousHealthMetadata.equals(healthMetadata) == false) {
                if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
                    taskQueue.submitTask("health-metadata-update", () -> this.healthMetadata, null);
                }
            }
        }
    }

    private void resetHealthMetadata(String source) {
        taskQueue.submitTask(source, () -> this.healthMetadata, null);
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
    interface UpsertHealthMetadataTask extends ClusterStateTaskListener {

        @Override
        default void onFailure(@Nullable Exception e) {
            logger.log(
                MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.WARN,
                () -> "failure during health metadata update",
                e
            );
        }

        default ClusterState execute(ClusterState currentState) {
            var initialHealthMetadata = HealthMetadata.getFromClusterState(currentState);
            var finalHealthMetadata = doExecute();
            return finalHealthMetadata.equals(initialHealthMetadata)
                ? currentState
                : currentState.copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, finalHealthMetadata));
        }

        HealthMetadata doExecute();

        class Executor extends SimpleBatchedExecutor<UpsertHealthMetadataTask, Void> {

            @Override
            public Tuple<ClusterState, Void> executeTask(UpsertHealthMetadataTask task, ClusterState clusterState) {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(UpsertHealthMetadataTask task, Void unused) {}
        }
    }

    private void updateOnDiskSettingsUpdated(String settingName, String value) {
        var diskBuilder = HealthMetadata.Disk.newBuilder(healthMetadata.getDiskMetadata());

        if (CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey().equals(settingName)) {
            diskBuilder.highWatermark(value, settingName);
        } else if (CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey().equals(settingName)) {
            diskBuilder.floodStageWatermark(value, settingName);
        } else if (CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey().equals(settingName)) {
            diskBuilder.frozenFloodStageWatermark(value, settingName);
        } else if (CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey().equals(settingName)) {
            diskBuilder.frozenFloodStageMaxHeadroom(value, settingName);
        } else if (CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey().equals(settingName)) {
            diskBuilder.highMaxHeadroom(value, settingName);
        } else if (CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey().equals(settingName)) {
            diskBuilder.floodStageMaxHeadroom(value, settingName);
        }

        this.previousHealthMetadata = healthMetadata;
        this.healthMetadata = HealthMetadata.newBuilder(previousHealthMetadata).disk(diskBuilder.build()).build();
    }

    private void updateOnShardLimitsSettingsUpdated(String settingName, Integer value) {
        var shardLimitsBuilder = HealthMetadata.ShardLimits.newBuilder(healthMetadata.getShardLimitsMetadata());

        if (SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey().equals(settingName)) {
            shardLimitsBuilder.maxShardsPerNode(value);
        } else if (SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey().equals(settingName)) {
            shardLimitsBuilder.maxShardsPerNodeFrozen(value);
        }
        this.previousHealthMetadata = healthMetadata;
        this.healthMetadata = HealthMetadata.newBuilder(previousHealthMetadata).shardLimits(shardLimitsBuilder.build()).build();
    }

    private static HealthMetadata initialHealthMetadata(Settings settings) {
        return new HealthMetadata(
            new HealthMetadata.Disk(
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.get(settings),
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.get(settings),
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.get(settings),
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.get(settings),
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.get(settings),
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.get(settings)
            ),
            new HealthMetadata.ShardLimits(
                SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(settings),
                SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.get(settings)
            )
        );
    }
}
