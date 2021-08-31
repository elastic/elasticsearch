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
    void onFailure(String source, Exception e);

    /**
     * A callback for when the task was rejected because the processing node is no longer the elected master.
     *
     * Implementations of this callback must not throw exceptions: an exception thrown here is logged by the master service at {@code ERROR}
     * level and otherwise ignored, except in tests where it raises an {@link AssertionError}. If log-and-ignore is the right behaviour then
     * implementations must do so themselves, typically using a more specific logger and at a less dramatic log level.
     */
    default void onNoLongerMaster(String source) {
        onFailure(source, new NotMasterException("no longer master. source: [" + source + "]"));
    }

    /**
     * Called when the result of the {@link ClusterStateTaskExecutor#execute(ClusterState, List)} have been processed
     * properly by all listeners.
     *
     * Implementations of this callback must not throw exceptions: an exception thrown here is logged by the master service at {@code ERROR}
     * level and otherwise ignored, except in tests where it raises an {@link AssertionError}. If log-and-ignore is the right behaviour then
     * implementations must do so themselves, typically using a more specific logger and at a less dramatic log level.
     */
    default void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
    }
}
