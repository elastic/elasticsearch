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

/**
 * A task that can update the cluster state.
 */
public abstract class ClusterStateUpdateTask implements ClusterStateTaskListener {

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

    /**
     * Computes the cluster state that results from executing this task on the given state. Returns the *same instance* if no change is
     * required, which is an important and valuable optimisation since it short-circuits the whole publication process and saves a bunch of
     * time and effort.
     */
    public abstract ClusterState execute(ClusterState currentState) throws Exception;

    /**
     * Called when the result of the {@link #execute} method has been processed properly by all listeners.
     *
     * The {@param newState} parameter is the state that was ultimately published.
     *
     * Implementations of this callback must not throw exceptions: an exception thrown here is logged by the master service at {@code ERROR}
     * level and otherwise ignored, except in tests where it raises an {@link AssertionError}. If log-and-ignore is the right behaviour then
     * implementations must do so themselves, typically using a more specific logger and at a less dramatic log level.
     */
    public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {}

    /**
     * If the cluster state update task wasn't processed by the provided timeout, call
     * {@link ClusterStateTaskListener#onFailure(Exception)}. May return null to indicate no timeout is needed (default).
     */
    @Nullable
    public final TimeValue timeout() {
        return timeout;
    }

    public final Priority priority() {
        return priority;
    }
}
