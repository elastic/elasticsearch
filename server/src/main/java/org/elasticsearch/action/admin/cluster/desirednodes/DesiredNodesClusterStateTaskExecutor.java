/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;

import java.util.List;

public class DesiredNodesClusterStateTaskExecutor implements ClusterStateTaskExecutor<ClusterStateUpdateTask> {
    @Override
    public ClusterTasksResult<ClusterStateUpdateTask> execute(ClusterState currentState, List<ClusterStateUpdateTask> tasks)
        throws Exception {
        ClusterStateTaskExecutor.ClusterTasksResult.Builder<ClusterStateUpdateTask> builder = ClusterStateTaskExecutor.ClusterTasksResult
            .builder();
        ClusterState clusterState = currentState;
        for (ClusterStateUpdateTask task : tasks) {
            try {
                clusterState = task.execute(clusterState);
                builder.success(task);
            } catch (Exception e) {
                builder.failure(task, e);
            }
        }
        return builder.build(clusterState);
    }
}
