/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.downsampling;

import org.elasticsearch.action.downsample.DownsampleShardTaskParams;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;

import java.util.Set;
import java.util.stream.Collectors;

public class DownsamplingOperationsMonitor {

    /**
     * Returns the names of indices in the given project that are currently being downsampled, determined by inspecting the
     * downsampling persistent tasks in the cluster state. We do not check the actual status of the task, so it is possible
     * that the data is slightly outdated, but eventually, all cancelled or completed are eventually removed from the cluster state.
     */
    public static Set<Index> getActivelyDownsampledIndexNames(ProjectMetadata project) {
        PersistentTasksCustomMetadata persistentTasks = PersistentTasksCustomMetadata.get(project);
        if (persistentTasks == null) {
            return Set.of();
        }
        return persistentTasks.findTasks(DownsampleShardTaskParams.NAME, task -> true)
            .stream()
            .map(task -> (DownsampleShardTaskParams) task.getParams())
            .filter(params -> params != null && params.shardId() != null)
            .map(params -> params.shardId().getIndex())
            .collect(Collectors.toUnmodifiableSet());
    }
}
