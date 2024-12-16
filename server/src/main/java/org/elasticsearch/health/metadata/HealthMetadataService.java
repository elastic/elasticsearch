/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.metadata;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.features.FeatureService;
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
    private final FeatureService featureService;
    private final ClusterStateListener clusterStateListener;
    private final MasterServiceTaskQueue<UpsertHealthMetadataTask> taskQueue;
    private volatile boolean enabled;

    // Allows us to know if this node is the elected master without checking the cluster state, effectively protecting
    // us from checking the cluster state before the cluster state is initialized
    private volatile boolean isMaster = false;
    // we hold an in-memory representation of the healthMetadata which will be updated by the settings updaters. This way, we can
    // get the initial values (reading default values from `Settings`) and then, potentially, user changed values (they'll be received
    // through the settings updater). Later and if this node is the master, we will always post the latest in-memory HealthMetadata to the
    // ClusterState to maintain an up-to-date version of it across the cluster.
    private volatile HealthMetadata localHealthMetadata;

    private HealthMetadataService(ClusterService clusterService, FeatureService featureService, Settings settings) {
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.clusterStateListener = this::updateOnClusterStateChange;
        this.enabled = ENABLED_SETTING.get(settings);
        this.localHealthMetadata = initialHealthMetadata(settings);
        this.taskQueue = clusterService.createTaskQueue("health metadata service", Priority.NORMAL, new Executor());
    }

    public static HealthMetadataService create(ClusterService clusterService, FeatureService featureService, Settings settings) {
        HealthMetadataService healthMetadataService = new HealthMetadataService(clusterService, featureService, settings);
        healthMetadataService.registerListeners();
        return healthMetadataService;
    }

    private void registerListeners() {
        if (this.enabled) {
            this.clusterService.addListener(clusterStateListener);
        }

        var clusterSettings = clusterService.getClusterSettings();

        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::updateOnHealthNodeEnabledChange);

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

    private void updateOnHealthNodeEnabledChange(boolean enabled) {
        this.enabled = enabled;
        if (this.enabled) {
            clusterService.addListener(clusterStateListener);

            if (canPostClusterStateUpdates(clusterService.state())) {
                taskQueue.submitTask("health-node-enabled", new UpsertHealthMetadataTask(), null);
            }
        } else {
            clusterService.removeListener(clusterStateListener);
        }
    }

    private boolean canPostClusterStateUpdates(ClusterState state) {
        // Wait until every node in the cluster supports health checks
        return isMaster && state.clusterRecovered();
    }

    private void updateOnClusterStateChange(ClusterChangedEvent event) {
        // Avoid scheduling updates to the taskQueue in case the cluster is recovering.
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        final boolean prevIsMaster = this.isMaster;
        if (prevIsMaster != event.localNodeMaster()) {
            this.isMaster = event.localNodeMaster();
        }
        if (canPostClusterStateUpdates(event.state())) {
            if (localHealthMetadata.equals(HealthMetadata.getFromClusterState(event.state())) == false) {
                taskQueue.submitTask("store-local-health-metadata", new UpsertHealthMetadataTask(), null);
            }
        }
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
    private static class UpsertHealthMetadataTask implements ClusterStateTaskListener {
        @Override
        public void onFailure(@Nullable Exception e) {
            logger.log(
                MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.WARN,
                () -> "failure during health metadata update",
                e
            );
        }
    }

    private class Executor extends SimpleBatchedExecutor<UpsertHealthMetadataTask, Void> {
        @Override
        public Tuple<ClusterState, Void> executeTask(UpsertHealthMetadataTask task, ClusterState clusterState) {
            final var initialHealthMetadata = HealthMetadata.getFromClusterState(clusterState);
            final var finalHealthMetadata = localHealthMetadata; // single volatile read
            return Tuple.tuple(
                finalHealthMetadata.equals(initialHealthMetadata)
                    ? clusterState
                    : clusterState.copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, finalHealthMetadata)),
                null
            );
        }

        @Override
        public void taskSucceeded(UpsertHealthMetadataTask task, Void unused) {}

        @Override
        public String describeTasks(List<UpsertHealthMetadataTask> tasks) {
            return ""; // tasks are equivalent and idempotent, no need to list them out
        }
    }

    private void updateOnDiskSettingsUpdated(String settingName, String value) {
        var diskBuilder = HealthMetadata.Disk.newBuilder(this.localHealthMetadata.getDiskMetadata());
        var healthMetadataBuilder = HealthMetadata.newBuilder(this.localHealthMetadata);

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

        this.localHealthMetadata = healthMetadataBuilder.disk(diskBuilder.build()).build();
    }

    private void updateOnShardLimitsSettingsUpdated(String settingName, Integer value) {
        var shardLimitsBuilder = HealthMetadata.ShardLimits.newBuilder(this.localHealthMetadata.getShardLimitsMetadata());
        var healthMetadataBuilder = HealthMetadata.newBuilder(this.localHealthMetadata);

        if (SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey().equals(settingName)) {
            shardLimitsBuilder.maxShardsPerNode(value);
        } else if (SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey().equals(settingName)) {
            shardLimitsBuilder.maxShardsPerNodeFrozen(value);
        }

        this.localHealthMetadata = healthMetadataBuilder.shardLimits(shardLimitsBuilder.build()).build();
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
