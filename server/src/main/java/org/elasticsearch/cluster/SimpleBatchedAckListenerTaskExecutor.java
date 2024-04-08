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
 * A basic batch executor implementation for tasks that can listen for acks themselves by providing a {@link ClusterStateAckListener}.
 * The tasks in the batch are executed iteratively, producing a cluster state after each task. This allows executing the tasks
 * in the batch as a series of executions, each taking an input cluster state and producing a new cluster state that serves as the
 * input of the next task in the batch.
 */
public abstract class SimpleBatchedAckListenerTaskExecutor<Task extends ClusterStateTaskListener>
    implements
        ClusterStateTaskExecutor<Task> {

    /**
     * Executes the provided task from the batch.
     *
     * @param task The task to be executed.
     * @param clusterState    The cluster state on which the task should be executed.
     * @return A tuple consisting of the resulting cluster state after executing this task, and a {@link ClusterStateAckListener}.
     * The listener is completed if the publication succeeds and the nodes ack the state update. The returned cluster state
     * serves as the cluster state on which the next task in the batch will run.
     */
    public abstract Tuple<ClusterState, ClusterStateAckListener> executeTask(Task task, ClusterState clusterState) throws Exception;

    /**
     * Called once all tasks in the batch have finished execution. It should return a cluster state that reflects
     * the execution of all the tasks.
     *
     * @param clusterState The cluster state resulting from the execution of all the tasks.
     * @param clusterStateChanged Whether {@code clusterState} is different from the cluster state before executing the tasks in the batch.
     * @return The resulting cluster state after executing all the tasks.
     */
    public ClusterState afterBatchExecution(ClusterState clusterState, boolean clusterStateChanged) {
        return clusterState;
    }

    @Override
    public final void clusterStatePublished(ClusterState newClusterState) {
        clusterStatePublished();
    }

    /**
     * Called after the new cluster state is published. Note that this method is not invoked if the cluster state was not updated.
     */
    public void clusterStatePublished() {}

    @Override
    public final ClusterState execute(BatchExecutionContext<Task> batchExecutionContext) throws Exception {
        var initState = batchExecutionContext.initialState();
        var clusterState = initState;
        for (final var taskContext : batchExecutionContext.taskContexts()) {
            try (var ignored = taskContext.captureResponseHeaders()) {
                var task = taskContext.getTask();
                final var result = executeTask(task, clusterState);
                clusterState = result.v1();
                taskContext.success(result.v2());
            } catch (Exception e) {
                taskContext.onFailure(e);
            }
        }
        try (var ignored = batchExecutionContext.dropHeadersContext()) {
            return afterBatchExecution(clusterState, clusterState != initState);
        }
    }
}
