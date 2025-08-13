/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentSLMMode;

/**
 * {@link UpdateSnapshotLifecycleStatsTask} is a cluster state update task that retrieves the
 * current SLM stats, merges them with the newly produced stats (non-mutating), and then updates
 * the cluster state with the new stats numbers
 */
public class UpdateSnapshotLifecycleStatsTask extends ClusterStateUpdateTask {
    private static final Logger logger = LogManager.getLogger(SnapshotRetentionTask.class);

    private final SnapshotLifecycleStats runStats;

    static final String TASK_SOURCE = "update_slm_stats";

    UpdateSnapshotLifecycleStatsTask(SnapshotLifecycleStats runStats) {
        this.runStats = runStats;
    }

    @Override
    public ClusterState execute(ClusterState currentState) {
        final Metadata currentMeta = currentState.metadata();
        final var project = currentMeta.getProject();
        final SnapshotLifecycleMetadata currentSlmMeta = project.custom(SnapshotLifecycleMetadata.TYPE);

        if (currentSlmMeta == null) {
            return currentState;
        }

        SnapshotLifecycleStats newMetrics = currentSlmMeta.getStats().merge(runStats);
        SnapshotLifecycleMetadata newSlmMeta = new SnapshotLifecycleMetadata(
            currentSlmMeta.getSnapshotConfigurations(),
            currentSLMMode(currentState),
            newMetrics
        );

        return currentState.copyAndUpdateProject(project.id(), builder -> builder.putCustom(SnapshotLifecycleMetadata.TYPE, newSlmMeta));
    }

    @Override
    public void onFailure(Exception e) {
        logger.error(
            () -> format(
                "failed to update cluster state with snapshot lifecycle stats, " + "source: [" + TASK_SOURCE + "], missing stats: [%s]",
                runStats
            ),
            e
        );
    }
}
