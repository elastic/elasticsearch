/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

/**
 * {@link UpdateSnapshotLifecycleStatsTask} is a cluster state update task that retrieves the
 * current SLM stats, merges them with the newly produced stats (non-mutating), and then updates
 * the cluster state with the new stats numbers
 */
public class UpdateSnapshotLifecycleStatsTask extends ClusterStateUpdateTask {
    private static final Logger logger = LogManager.getLogger(SnapshotRetentionTask.class);

    private final SnapshotLifecycleStats runStats;

    UpdateSnapshotLifecycleStatsTask(SnapshotLifecycleStats runStats) {
        this.runStats = runStats;
    }

    @Override
    public ClusterState execute(ClusterState currentState) {
        final MetaData currentMeta = currentState.metaData();
        final SnapshotLifecycleMetadata currentSlmMeta = currentMeta.custom(SnapshotLifecycleMetadata.TYPE);

        if (currentSlmMeta == null) {
            return currentState;
        }

        SnapshotLifecycleStats newMetrics = currentSlmMeta.getStats().merge(runStats);
        SnapshotLifecycleMetadata newSlmMeta = new SnapshotLifecycleMetadata(currentSlmMeta.getSnapshotConfigurations(),
            currentSlmMeta.getOperationMode(), newMetrics);

        return ClusterState.builder(currentState)
            .metaData(MetaData.builder(currentMeta)
                .putCustom(SnapshotLifecycleMetadata.TYPE, newSlmMeta))
            .build();
    }

    @Override
    public void onFailure(String source, Exception e) {
        logger.error(new ParameterizedMessage("failed to update cluster state with snapshot lifecycle stats, " +
            "source: [{}], missing stats: [{}]", source, runStats),
            e);
    }
}
