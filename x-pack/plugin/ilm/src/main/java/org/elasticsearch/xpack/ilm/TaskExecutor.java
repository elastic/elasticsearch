/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;

import java.util.List;

class TaskExecutor implements ClusterStateTaskExecutor<ClusterStateUpdateTask> {

    @Override
    public ClusterTasksResult<ClusterStateUpdateTask> execute(ClusterState currentState, List<ClusterStateUpdateTask> tasks)
        throws Exception {
        ClusterTasksResult.Builder<ClusterStateUpdateTask> builder = ClusterTasksResult.builder();
        ClusterState state = currentState;
        for (ClusterStateUpdateTask task : tasks) {
            state = task.execute(state);
            builder.success(task);
        }
        return builder.build(state);
    }

    @Override
    public String describeTasks(List<ClusterStateUpdateTask> tasks) {
        return "";
    }
}
