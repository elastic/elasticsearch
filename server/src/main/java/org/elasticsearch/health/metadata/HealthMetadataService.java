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
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;

import static org.elasticsearch.health.node.selection.HealthNodeTaskExecutor.ENABLED_SETTING;

/**
 * Keeps the health metadata in the cluster state up to date. It listens to master elections and changes in the disk thresholds.
 */
public class HealthMetadataService {

    private static final Logger logger = LogManager.getLogger(HealthMetadataService.class);

    private final ClusterService clusterService;
    private final DiskThresholdSettings diskThresholdSettings;

    private final ClusterStateListener clusterStateListener;
    private final DiskThresholdSettings.ChangedThresholdListener diskThresholdListener;

    private volatile boolean enabled;

    private volatile boolean publishedAfterElection = false;

    private final ClusterStateTaskExecutor<UpdateHealthMetadataTask> taskExecutor = new UpdateHealthMetadataTask.Executor();

    public HealthMetadataService(DiskThresholdSettings diskThresholdSettings, ClusterService clusterService, Settings settings) {
        this.clusterService = clusterService;
        this.diskThresholdSettings = diskThresholdSettings;
        this.clusterStateListener = this::updateHealthMetadataIfNecessary;
        this.diskThresholdListener = this::updateHealthMetadataIfNecessary;
        this.enabled = ENABLED_SETTING.get(settings);
        if (this.enabled) {
            this.clusterService.addListener(clusterStateListener);
            this.diskThresholdSettings.addListener(diskThresholdListener);
        }
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENABLED_SETTING, this::enable);
    }

    private void enable(boolean enabled) {
        this.enabled = enabled;
        if (this.enabled) {
            clusterService.addListener(clusterStateListener);
            diskThresholdSettings.addListener(diskThresholdListener);
        } else {
            clusterService.removeListener(clusterStateListener);
            diskThresholdSettings.removeListener(diskThresholdListener);
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

    private void updateHealthMetadataIfNecessary() {
        ClusterState clusterState = clusterService.state();
        if (clusterState.nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_4_0)
            && clusterState.nodes().isLocalNodeElectedMaster()) {
            submitHealthMetadata("health-metadata-update-threshold-change");
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

    private HealthMetadata createHealthMetadata() {
        return new HealthMetadata(HealthMetadata.DiskThresholds.createDiskThresholds(diskThresholdSettings));
    }

    private void submitHealthMetadata(String source) {
        HealthMetadata localHealthMedata = createHealthMetadata();
        var task = new UpdateHealthMetadataTask(localHealthMedata);
        var config = ClusterStateTaskConfig.build(Priority.NORMAL);
        clusterService.submitStateUpdateTask(source, task, config, taskExecutor);
    }

    record UpdateHealthMetadataTask(HealthMetadata healthMetadata) implements ClusterStateTaskListener {

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            assert false : "never called";
        }

        @Override
        public void onFailure(@Nullable Exception e) {
            logger.error("failure during health metadata update", e);
        }

        static class Executor implements ClusterStateTaskExecutor<UpdateHealthMetadataTask> {

            @Override
            public ClusterState execute(ClusterState currentState, List<TaskContext<UpdateHealthMetadataTask>> taskContexts)
                throws Exception {
                final HealthMetadata initialHealthMetadata = HealthMetadata.getHealthCustomMetadata(currentState);
                HealthMetadata currentHealthMetadata = initialHealthMetadata;
                for (TaskContext<UpdateHealthMetadataTask> taskContext : taskContexts) {
                    currentHealthMetadata = taskContext.getTask().healthMetadata();
                    taskContext.success(() -> {});
                }
                final var finalHealthMetadata = currentHealthMetadata;
                return finalHealthMetadata == initialHealthMetadata
                    ? currentState
                    : currentState.copyAndUpdateMetadata(b -> b.putCustom(HealthMetadata.TYPE, finalHealthMetadata));
            }
        }
    }
}
