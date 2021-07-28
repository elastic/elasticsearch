/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;

import java.util.List;

/**
 * A task that can update the cluster state.
 */
public abstract class ClusterStateUpdateTask
        implements ClusterStateTaskConfig, ClusterStateTaskExecutor<ClusterStateUpdateTask>, ClusterStateTaskListener {

    private final Priority priority;

    @Nullable
    private final TimeValue timeout;

    public ClusterStateUpdateTask() {
        this(Priority.NORMAL);
    }

    public ClusterStateUpdateTask(Priority priority) {
        this(priority, null);
    }

    public ClusterStateUpdateTask(TimeValue timeout) {
        this(Priority.NORMAL, timeout);
    }

    public ClusterStateUpdateTask(Priority priority, TimeValue timeout) {
        this.priority = priority;
        this.timeout = timeout;
    }

    @Override
    public final ClusterTasksResult<ClusterStateUpdateTask> execute(ClusterState currentState, List<ClusterStateUpdateTask> tasks)
            throws Exception {
        ClusterState result = execute(currentState);
        return ClusterTasksResult.<ClusterStateUpdateTask>builder().successes(tasks).build(result);
    }

    @Override
    public String describeTasks(List<ClusterStateUpdateTask> tasks) {
        return ""; // one of task, source is enough
    }

    /**
     * Update the cluster state based on the current state. Return the *same instance* if no state
     * should be changed.
     */
    public abstract ClusterState execute(ClusterState currentState) throws Exception;

    /**
     * A callback for when task execution fails.
     *
     * Implementations of this callback should not throw exceptions: an exception thrown here is logged by the master service at {@code
     * ERROR} level and otherwise ignored. If log-and-ignore is the right behaviour then implementations should do so themselves, typically
     * using a more specific logger and at a less dramatic log level.
     */
    public abstract void onFailure(String source, Exception e);

    @Override
    public final void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
        // final, empty implementation here as this method should only be defined in combination
        // with a batching executor as it will always be executed within the system context.
    }

    /**
     * If the cluster state update task wasn't processed by the provided timeout, call
     * {@link ClusterStateTaskListener#onFailure(String, Exception)}. May return null to indicate no timeout is needed (default).
     */
    @Nullable
    public final TimeValue timeout() {
        return timeout;
    }

    @Override
    public final Priority priority() {
        return priority;
    }

    /**
     * Marked as final as cluster state update tasks should only run on master.
     * For local requests, use {@link LocalClusterUpdateTask} instead.
     */
    @Override
    public final boolean runOnlyOnMaster() {
        return true;
    }
}
