/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.service.MasterService;

public interface ClusterStateTaskListener {
    /**
     * A callback for when task execution fails. May receive a {@link NotMasterException} if this node stopped being the master before this
     * task was executed or a {@link ProcessClusterEventTimeoutException} if the task timed out before it was executed. If the task fails
     * during execution then this method receives the corresponding exception. If the task executes successfully but the resulting cluster
     * state publication fails then this method receives a {@link FailedToCommitClusterStateException}. If publication fails then a new
     * master is elected and the update might or might not take effect, depending on whether or not the newly-elected master accepted the
     * published state that failed to be committed.
     * <p>
     * Use {@link MasterService#isPublishFailureException} to detect the "expected" master failure cases if needed.
     * <p>
     * Implementations of this callback must not throw exceptions: an exception thrown here is logged by the master service at {@code ERROR}
     * level and otherwise ignored, except in tests where it raises an {@link AssertionError}. If log-and-ignore is the right behaviour then
     * implementations must do so themselves, typically using a more specific logger and at a less dramatic log level.
     */
    void onFailure(Exception e);
}
