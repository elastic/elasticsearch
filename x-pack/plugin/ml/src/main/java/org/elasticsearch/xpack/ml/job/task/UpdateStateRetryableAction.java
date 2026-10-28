/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.task;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;

import java.util.Objects;

/**
 * A {@link RetryableAction} that robustly persists a {@link JobTaskState} to cluster state.
 *
 * Retries on all exceptions except {@link ResourceNotFoundException}, which indicates the
 * persistent task no longer exists and retrying would be pointless.
 *
 * Two timeout modes:
 * <ul>
 *   <li>Default (no timeout arg): uses {@link MlTasks#PERSISTENT_TASK_MASTER_NODE_TIMEOUT} (365 days) --
 *       appropriate for long-running state transitions like OPENED where a native process is running.</li>
 *   <li>Custom timeout: use for bounded transitions like FAILED during the opening phase (e.g. 60 minutes).</li>
 * </ul>
 *
 * <p><b>Design decision: why different timeouts?</b> For FAILED, no native process is running; we are
 * only recording the failure. A bounded timeout (e.g. 60 minutes) is acceptable: if the master is
 * unavailable that long, we eventually fall back to {@code markAsFailed()}. For OPENED, a native
 * process is already running and we must commit the state so cluster state matches reality. A bounded
 * timeout would orphan the process (running process, cluster state stuck in OPENING). We therefore
 * use an effectively unbounded timeout (365 days) for OPENED so we keep retrying until the master
 * acknowledges the transition.
 */
public class UpdateStateRetryableAction extends RetryableAction<PersistentTasksCustomMetadata.PersistentTask<?>> {

    private static final int MIN_RETRY_SLEEP_MILLIS = 500;

    private final JobTask jobTask;
    private final JobTaskState jobTaskState;

    /**
     * Default constructor -- uses {@link MlTasks#PERSISTENT_TASK_MASTER_NODE_TIMEOUT} (365 days).
     * Appropriate for OPENED state transitions where a native process is running.
     */
    public UpdateStateRetryableAction(
        Logger logger,
        ThreadPool threadPool,
        JobTask jobTask,
        JobTaskState jobTaskState,
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> delegateListener
    ) {
        this(logger, threadPool, jobTask, jobTaskState, MlTasks.PERSISTENT_TASK_MASTER_NODE_TIMEOUT, delegateListener);
    }

    /**
     * Custom timeout constructor -- use when a bounded retry window is appropriate (e.g. 60 minutes for FAILED state).
     */
    public UpdateStateRetryableAction(
        Logger logger,
        ThreadPool threadPool,
        JobTask jobTask,
        JobTaskState jobTaskState,
        TimeValue timeout,
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> delegateListener
    ) {
        super(logger, threadPool, TimeValue.timeValueMillis(MIN_RETRY_SLEEP_MILLIS), timeout, delegateListener, threadPool.generic());
        this.jobTask = Objects.requireNonNull(jobTask);
        this.jobTaskState = Objects.requireNonNull(jobTaskState);
    }

    @Override
    public void tryAction(ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener) {
        jobTask.updatePersistentTaskState(jobTaskState, listener);
    }

    @Override
    public boolean shouldRetry(Exception e) {
        return (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) == false;
    }
}
