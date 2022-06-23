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
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;
import java.util.Objects;

/**
 * Keeps the health metadata in the cluster state up to date. It listens to master elections and changes in the disk thresholds.
 */
public class HealthMetadataService {

    // Visible for testing
    static final Logger logger = LogManager.getLogger(HealthMetadataService.class);
    private static final int MAX_RETRIES = 5;

    private final ClusterService clusterService;
    private final DiskThresholdSettings diskThresholdSettings;

    private volatile boolean publishedAfterElection = false;
    private final ClusterStateTaskExecutor<UpdateHealthMetadataTask> taskExecutor = new UpdateHealthMetadataTask.Executor();

    public HealthMetadataService(DiskThresholdSettings diskThresholdSettings, ClusterService clusterService) {
        this.clusterService = clusterService;
        this.diskThresholdSettings = diskThresholdSettings;
        this.clusterService.addListener(this::updateHealthMetadataIfNecessary);
        this.diskThresholdSettings.addListener(this::updateHealthMetadataIfNecessary);
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

    private void updateHealthMetadataIfNecessary(DiskThresholdSettings ignored) {
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
        return new HealthMetadata(HealthMetadata.DiskHealthThresholds.from(diskThresholdSettings));
    }

    private void submitHealthMetadata(String source) {
        submitHealthMetadata(source, 0);
    }

    // Visible for testing
    void submitHealthMetadata(String source, int attempt) {
        if (attempt < MAX_RETRIES) {
            HealthMetadata localHealthMedata = createHealthMetadata();
            String message = attempt == 0 ? source : String.format("retry[%d]-%s", attempt, source);
            var task = new UpdateHealthMetadataTask(localHealthMedata, () -> submitHealthMetadata(source, attempt + 1));
            var config = ClusterStateTaskConfig.build(Priority.NORMAL);
            clusterService.submitStateUpdateTask(message, task, config, taskExecutor);
        } else {
            logger.error("Failed to process {} after {} attempts", source, attempt);
        }
    }

    record UpdateHealthMetadataTask(HealthMetadata healthMetadata, Runnable retry) implements ClusterStateTaskListener {

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            assert false : "never called";
        }

        @Override
        public void onFailure(@Nullable Exception e) {
            logger.error("unexpected failure during health metadata update", e);
            retry.run();
        }

        static class Executor implements ClusterStateTaskExecutor<UpdateHealthMetadataTask> {

            @Override
            public ClusterState execute(ClusterState currentState, List<TaskContext<UpdateHealthMetadataTask>> taskContexts)
                throws Exception {
                final HealthMetadata originalHealthMetadata = HealthMetadata.getHealthCustomMetadata(currentState);
                HealthMetadata mostRecentHealthMetadata = originalHealthMetadata;
                if (taskContexts.isEmpty() == false) {
                    mostRecentHealthMetadata = taskContexts.get(taskContexts.size() - 1).getTask().healthMetadata();
                    if (Objects.equals(originalHealthMetadata, mostRecentHealthMetadata)) {
                        mostRecentHealthMetadata = originalHealthMetadata;
                    }
                }
                for (final var taskContext : taskContexts) {
                    taskContext.success(() -> {});
                }
                return originalHealthMetadata == mostRecentHealthMetadata
                    ? currentState
                    : ClusterState.builder(currentState)
                        .metadata(Metadata.builder(currentState.metadata()).putCustom(HealthMetadata.TYPE, mostRecentHealthMetadata))
                        .build();
            }
        }
    }
}
