/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.datastreams.DownsamplingOperations;
import org.elasticsearch.index.Index;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.downsample.DownsampleShardPersistentTaskState;

import java.util.HashSet;
import java.util.Set;

public class DownsamplingOperationsMonitor implements DownsamplingOperations {

    /**
     * Returns the names of indices in the given project that are currently being downsampled, determined by inspecting the
     * downsampling persistent tasks in the cluster state. We do not check the actual status of the task, so it is possible
     * that the data is slightly outdated, but eventually, all cancelled or completed are eventually removed from the cluster state.
     */
    public Set<Index> getActivelyDownsampledIndexNames(ProjectMetadata project) {
        PersistentTasksCustomMetadata persistentTasks = PersistentTasksCustomMetadata.get(project);
        if (persistentTasks == null) {
            return Set.of();
        }
        Set<Index> indicesInProgress = new HashSet<>();
        for (var task : persistentTasks.findTasks(DownsampleShardTaskParams.NAME, task -> true)) {
            if (task.getParams() instanceof DownsampleShardTaskParams params) {
                if (isDownsamplingInProgress(task) && params.shardId() != null) {
                    indicesInProgress.add(params.shardId().getIndex());
                }
            }
        }
        return indicesInProgress;
    }

    private boolean isDownsamplingInProgress(PersistentTasksCustomMetadata.PersistentTask<?> task) {
        PersistentTaskState taskState = task.getState();
        if (taskState instanceof DownsampleShardPersistentTaskState downsamplingState) {
            return downsamplingState.done() == false;
        }
        return true;
    }
}
