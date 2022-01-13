/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.List;

/**
 * A task that can update the cluster state.
 */
public abstract class ClusterStateUpdateTask
    implements
        ClusterStateTaskConfig,
        ClusterStateTaskExecutor<ClusterStateUpdateTask>,
        ClusterStateTaskListener {

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
     * Computes the cluster state that results from executing this task on the given state. Returns the *same instance* if no change is
     * required, which is an important and valuable optimisation since it short-circuits the whole publication process and saves a bunch of
     * time and effort.
     */
    public abstract ClusterState execute(ClusterState currentState) throws Exception;

    /**
     * A callback for when task execution fails.
     * <p>
     * Implementations of this callback should not throw exceptions: an exception thrown here is logged by the master service at {@code
     * ERROR} level and otherwise ignored. If log-and-ignore is the right behaviour then implementations should do so themselves, typically
     * using a more specific logger and at a less dramatic log level.
     */
    public abstract void onFailure(String source, Exception e);

    @Override
    public final void clusterStatePublished(ClusterStatePublicationEvent clusterStatePublicationEvent) {
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
