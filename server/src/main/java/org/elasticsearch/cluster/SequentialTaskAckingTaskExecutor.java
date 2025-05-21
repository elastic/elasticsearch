/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.core.Tuple;

/**
 * A task executor that executes tasks sequentially, allowing each task to acknowledge the cluster state update.
 * This executor is used for tasks that need to be executed one after another, where each task can produce a new
 * cluster state and can listen for acknowledgments.
 *
 * @param <Task> The type of the task that extends {@link AckedClusterStateUpdateTask}.
 */
public class SequentialTaskAckingTaskExecutor<Task extends AckedClusterStateUpdateTask> extends SimpleBatchedAckListenerTaskExecutor<Task> {
    @Override
    public Tuple<ClusterState, ClusterStateAckListener> executeTask(Task task, ClusterState clusterState) throws Exception {
        return Tuple.tuple(task.execute(clusterState), task);
    }
}
