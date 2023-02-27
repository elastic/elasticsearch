/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

/**
 * A queue of tasks for the master service to execute. Tasks submitted to the same queue can be processed as a batch, resulting in a single
 * cluster state update. Queues are typically created during initialization using {@link MasterService#createTaskQueue}.
 *
 * @param <T> The type of task to process.
 */
public interface MasterServiceTaskQueue<T extends ClusterStateTaskListener> {

    /**
     * Submit a task to the queue.
     *
     * @param source A description of the task.
     *
     * @param task The task to execute.
     *
     * @param timeout An optional timeout for the task. If the task is not processed before the timeout elapses, it fails with a {@link
     *                ProcessClusterEventTimeoutException} (which is passed to {@link ClusterStateTaskListener#onFailure}). Tasks that are
     *                directly associated with user actions conventionally use a timeout which comes from the REST parameter {@code
     *                ?master_timeout}, which is typically available from {@link MasterNodeRequest#masterNodeTimeout()}. Tasks that
     *                correspond with internal actions should normally have no timeout since it is usually better to wait patiently in the
     *                queue until processed rather than to fail, especially if the only reasonable reaction to a failure is to retry.
     */
    void submitTask(String source, T task, @Nullable TimeValue timeout);
}
