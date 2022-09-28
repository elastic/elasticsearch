/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.core.Tuple;

/**
 * A {@link BatchedTaskExecutor} for tasks that do not return any value as a result of their execution, other than the next cluster state.
 */
public abstract class BatchedSimpleTaskExecutor<Task extends ClusterStateTaskListener> extends BatchedTaskExecutor<Task, Void> {

    @Override
    public final Tuple<ClusterState, Void> executeTask(Task task, ClusterState clusterState) throws Exception {
        return Tuple.tuple(executeSimpleTask(task, clusterState), null);
    }

    @Override
    public final void taskSucceeded(Task task, Void unused) {
        taskSucceeded(task);
    }

    public abstract ClusterState executeSimpleTask(Task task, ClusterState clusterState) throws Exception;

    public abstract void taskSucceeded(Task task);
}
