/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.snapshot.upgrader;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Optional;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutor.checkAssignmentState;

public class SnapshotUpgradePredicate implements Predicate<PersistentTasksCustomMetadata.PersistentTask<?>> {
    private final boolean waitForCompletion;
    private final Logger logger;
    private volatile Exception exception;
    private volatile String node = "";
    private volatile boolean shouldCancel;
    private volatile boolean isCompleted;

    public SnapshotUpgradePredicate(boolean waitForCompletion, Logger logger) {
        this.waitForCompletion = waitForCompletion;
        this.logger = logger;
    }

    public Exception getException() {
        return exception;
    }

    public String getNode() {
        return node;
    }

    public boolean isShouldCancel() {
        return shouldCancel;
    }

    public boolean isCompleted() {
        return isCompleted;
    }

    @Override
    public boolean test(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
        // Persistent task being null means it has been removed from state, and is now complete
        if (persistentTask == null) {
            isCompleted = true;
            return true;
        }
        SnapshotUpgradeTaskState snapshotUpgradeTaskState = (SnapshotUpgradeTaskState) persistentTask.getState();
        SnapshotUpgradeState snapshotUpgradeState = snapshotUpgradeTaskState == null
            ? SnapshotUpgradeState.STOPPED
            : snapshotUpgradeTaskState.getState();
        String reason = snapshotUpgradeTaskState == null ? "" : snapshotUpgradeTaskState.getReason();
        PersistentTasksCustomMetadata.Assignment assignment = persistentTask.getAssignment();
        // This logic is only appropriate when opening a job, not when reallocating following a failure,
        // and this is why this class must only be used when opening a job
        SnapshotUpgradeTaskParams params = (SnapshotUpgradeTaskParams) persistentTask.getParams();
        Optional<ElasticsearchException> assignmentException = checkAssignmentState(assignment, params.getJobId(), logger);
        if (assignmentException.isPresent()) {
            exception = assignmentException.get();
            shouldCancel = true;
            return true;
        }
        if (snapshotUpgradeState == SnapshotUpgradeState.FAILED) {
            exception = ExceptionsHelper.serverError(
                "Unexpected state ["
                    + snapshotUpgradeState
                    + "] while waiting for to be assigned to a node; recorded reason ["
                    + reason
                    + "]"
            );
            shouldCancel = true;
            return true;
        }
        if (persistentTask.getExecutorNode() != null) {
            node = persistentTask.getExecutorNode();
            // If waitForCompletion is true, we need to wait for the task to be finished. Otherwise, return true once it is assigned
            return waitForCompletion == false;
        }
        return false;
    }
}
