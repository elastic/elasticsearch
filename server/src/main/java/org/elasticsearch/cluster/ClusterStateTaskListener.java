/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import java.util.List;

public interface ClusterStateTaskListener {

    /**
     * A callback for when task execution fails.
     *
     * Implementations of this callback must not throw exceptions: an exception thrown here is logged by the master service at {@code ERROR}
     * level and otherwise ignored, except in tests where it raises an {@link AssertionError}. If log-and-ignore is the right behaviour then
     * implementations must do so themselves, typically using a more specific logger and at a less dramatic log level.
     */
    void onFailure(Exception e);

    /**
     * A callback for when the task was rejected because the processing node is no longer the elected master.
     *
     * Implementations of this callback must not throw exceptions: an exception thrown here is logged by the master service at {@code ERROR}
     * level and otherwise ignored, except in tests where it raises an {@link AssertionError}. If log-and-ignore is the right behaviour then
     * implementations must do so themselves, typically using a more specific logger and at a less dramatic log level.
     */
    default void onNoLongerMaster() {
        onFailure(new NotMasterException("no longer master"));
    }

    /**
     * Called when the result of the {@link ClusterStateTaskExecutor#execute(ClusterState, List)} method have been processed properly by all
     * listeners.
     *
     * The {@param newState} parameter is the state that was ultimately published. This can lead to surprising behaviour if tasks are
     * batched together: a later task in the batch may undo or overwrite the changes made by an earlier task. In general you should prefer
     * to ignore the published state and instead handle the success of a publication via the listener that the executor passes to
     * {@link ClusterStateTaskExecutor.TaskContext#success}.
     *
     * Implementations of this callback must not throw exceptions: an exception thrown here is logged by the master service at {@code ERROR}
     * level and otherwise ignored, except in tests where it raises an {@link AssertionError}. If log-and-ignore is the right behaviour then
     * implementations must do so themselves, typically using a more specific logger and at a less dramatic log level.
     */
    // TODO: replace all remaining usages of this method with dedicated listeners and then remove it.
    default void clusterStateProcessed(ClusterState oldState, ClusterState newState) {}
}
