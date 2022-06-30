/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.DiskThresholdSettingParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;
import static org.elasticsearch.health.node.selection.HealthNodeTaskExecutor.ENABLED_SETTING;

/**
 * Keeps the health metadata in the cluster state up to date. It listens to master elections and changes in the disk thresholds.
 */
public class HealthMetadataService {

    private static final Logger logger = LogManager.getLogger(HealthMetadataService.class);

    private final ClusterService clusterService;
    private final ClusterStateListener clusterStateListener;
    private final DiskHealthMetadataMonitor diskHealthMetadataMonitor;
    private volatile boolean enabled;
    private volatile boolean publishedAfterElection = false;

    private final ClusterStateTaskExecutor<UpdateHealthMetadataTask> taskExecutor = new UpdateHealthMetadataTask.Executor();

    public HealthMetadataService(ClusterService clusterService, Settings settings) {
        this.clusterService = clusterService;
        this.diskHealthMetadataMonitor = new DiskHealthMetadataMonitor(
            settings,
            clusterService.getClusterSettings(),
            this::updateHealthMetadata
        );
        this.clusterStateListener = this::updateHealthMetadataIfNecessary;
        this.enabled = ENABLED_SETTING.get(settings);
        if (this.enabled) {
            this.clusterService.addListener(clusterStateListener);
        }
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENABLED_SETTING, this::enable);
    }

    private void enable(boolean enabled) {
        this.enabled = enabled;
        if (this.enabled) {
            clusterService.addListener(clusterStateListener);
            updateHealthMetadata();
        } else {
            clusterService.removeListener(clusterStateListener);
            publishedAfterElection = false;
        }
    }

    private void updateHealthMetadataIfNecessary(ClusterChangedEvent event) {
        // Wait until every node in the cluster is upgraded to 8.4.0 or later
        if (event.state().nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_4_0)) {
            if (event.localNodeMaster() && publishedAfterElection == false) {
                submitHealthMetadata("health-metadata-update-master-election");
            }
            // If the node is not the elected master anymore
            publishedAfterElection = event.localNodeMaster();
        }
    }

    private void updateHealthMetadata() {
        // The first trigger to update the health metadata should be a master election because
        // this means the cluster is fully initialized
        if (publishedAfterElection && enabled) {
            ClusterState clusterState = clusterService.state();
            if (clusterState.nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_4_0)
                && clusterState.nodes().isLocalNodeElectedMaster()) {
                submitHealthMetadata("health-metadata-update");
            }
        }
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return List.of(
            new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(HealthMetadata.TYPE), HealthMetadata::fromXContent)
        );
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, HealthMetadata.TYPE, HealthMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, HealthMetadata.TYPE, HealthMetadata::readDiffFrom)
        );
    }

    private void submitHealthMetadata(String source) {
        var task = new UpdateHealthMetadataTask(diskHealthMetadataMonitor);
        var config = ClusterStateTaskConfig.build(Priority.NORMAL);
        clusterService.submitStateUpdateTask(source, task, config, taskExecutor);
    }

    record UpdateHealthMetadataTask(DiskHealthMetadataMonitor diskHealthMetadataMonitor) implements ClusterStateTaskListener {

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            assert false : "never called";
        }

        @Override
        public void onFailure(@Nullable Exception e) {
            logger.error("failure during health metadata update", e);
        }

        private HealthMetadata execute() {
            return new HealthMetadata(diskHealthMetadataMonitor.getDiskHealthMetadata());
        }

        static class Executor implements ClusterStateTaskExecutor<UpdateHealthMetadataTask> {

            @Override
            public ClusterState execute(ClusterState currentState, List<TaskContext<UpdateHealthMetadataTask>> taskContexts)
                throws Exception {
                final HealthMetadata initialHealthMetadata = HealthMetadata.getHealthCustomMetadata(currentState);
                HealthMetadata currentHealthMetadata = initialHealthMetadata;
                for (TaskContext<UpdateHealthMetadataTask> taskContext : taskContexts) {
                    currentHealthMetadata = taskContext.getTask().execute();
                    taskContext.success(() -> {});
                }
                final var finalHealthMetadata = currentHealthMetadata;
                return finalHealthMetadata == initialHealthMetadata
                    ? currentState
                    : currentState.copyAndUpdateMetadata(b -> b.putCustom(HealthMetadata.TYPE, finalHealthMetadata));
            }
        }
    }

    /**
     * Monitors and caches the current values of the disk thresholds. Upon a setting change
     * it runs the callback method.
     */
    static class DiskHealthMetadataMonitor {
        private final Runnable callback;
        private volatile Double watermarkLow;
        private volatile Double watermarkHigh;
        private volatile ByteSizeValue freeBytesWatermarkLow;
        private volatile ByteSizeValue freeBytesWatermarkHigh;
        private volatile Double watermarkFloodStage;
        private volatile ByteSizeValue freeBytesWatermarkFloodStage;
        private volatile RelativeByteSizeValue frozenFloodStage;
        private volatile ByteSizeValue frozenFloodStageMaxHeadroom;

        DiskHealthMetadataMonitor(Settings settings, ClusterSettings clusterSettings, Runnable callback) {
            this.callback = callback;
            setLowWatermark(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.get(settings));
            setHighWatermark(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.get(settings));
            setFloodStage(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.get(settings));
            setFrozenFloodStage(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_SETTING.get(settings));
            setFrozenFloodStageMaxHeadroom(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.get(settings));
            clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING, this::setLowWatermark);
            clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING, this::setHighWatermark);
            clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING, this::setFloodStage);
            clusterSettings.addSettingsUpdateConsumer(
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_SETTING,
                this::setFrozenFloodStage
            );
            clusterSettings.addSettingsUpdateConsumer(
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING,
                this::setFrozenFloodStageMaxHeadroom
            );
        }

        private void setLowWatermark(String lowWatermark) {
            this.watermarkLow = DiskThresholdSettingParser.parseThresholdPercentage(lowWatermark);
            this.freeBytesWatermarkLow = DiskThresholdSettingParser.parseThresholdBytes(
                lowWatermark,
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey()
            );
            callback.run();
        }

        private void setHighWatermark(String highWatermark) {
            this.watermarkHigh = DiskThresholdSettingParser.parseThresholdPercentage(highWatermark);
            this.freeBytesWatermarkHigh = DiskThresholdSettingParser.parseThresholdBytes(
                highWatermark,
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey()
            );
            callback.run();
        }

        private void setFloodStage(String floodStageRaw) {
            this.watermarkFloodStage = DiskThresholdSettingParser.parseThresholdPercentage(floodStageRaw);
            this.freeBytesWatermarkFloodStage = DiskThresholdSettingParser.parseThresholdBytes(
                floodStageRaw,
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey()
            );
            callback.run();
        }

        private void setFrozenFloodStage(RelativeByteSizeValue floodStage) {
            this.frozenFloodStage = floodStage;
            callback.run();
        }

        private void setFrozenFloodStageMaxHeadroom(ByteSizeValue maxHeadroom) {
            this.frozenFloodStageMaxHeadroom = maxHeadroom;
            callback.run();
        }

        Double getWatermarkLow() {
            return watermarkLow;
        }

        Double getWatermarkHigh() {
            return watermarkHigh;
        }

        ByteSizeValue getFreeBytesWatermarkLow() {
            return freeBytesWatermarkLow;
        }

        ByteSizeValue getFreeBytesWatermarkHigh() {
            return freeBytesWatermarkHigh;
        }

        Double getWatermarkFloodStage() {
            return watermarkFloodStage;
        }

        ByteSizeValue getFreeBytesWatermarkFloodStage() {
            return freeBytesWatermarkFloodStage;
        }

        RelativeByteSizeValue getFrozenFloodStage() {
            return frozenFloodStage;
        }

        ByteSizeValue getFrozenFloodStageMaxHeadroom() {
            return frozenFloodStageMaxHeadroom;
        }

        HealthMetadata.Disk getDiskHealthMetadata() {
            return new HealthMetadata.Disk(
                new HealthMetadata.Disk.Threshold(getWatermarkLow(), getFreeBytesWatermarkLow()),
                new HealthMetadata.Disk.Threshold(getWatermarkHigh(), getFreeBytesWatermarkHigh()),
                new HealthMetadata.Disk.Threshold(getWatermarkFloodStage(), getFreeBytesWatermarkFloodStage()),
                new HealthMetadata.Disk.Threshold(getFrozenFloodStage()),
                getFrozenFloodStageMaxHeadroom()
            );
        }
    }
}
