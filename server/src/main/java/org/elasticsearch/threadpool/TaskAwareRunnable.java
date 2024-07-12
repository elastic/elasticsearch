

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.threadpool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.WrappedRunnable;
import org.elasticsearch.tasks.TaskManager;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Thread.currentThread;
import static org.elasticsearch.tasks.TaskResourceTrackingService.TASK_ID;

/**
 * Responsible for wrapping the original task's runnable and sending updates on when it starts and finishes to
 * entities listening to the events.
 * <p>
 * It's able to associate runnable with a task with the help of task Id available in thread context.
 */
public class TaskAwareRunnable extends AbstractRunnable implements WrappedRunnable {

    private static final Logger logger = LogManager.getLogger(TaskManager.class);

    private final Runnable original;
    private final ThreadContext threadContext;
    private final AtomicReference<RunnableTaskExecutionListener> runnableTaskListener;

    public TaskAwareRunnable(
        final ThreadContext threadContext,
        final Runnable original,
        final AtomicReference<RunnableTaskExecutionListener> runnableTaskListener
    ) {
        this.original = original;
        this.threadContext = threadContext;
        this.runnableTaskListener = runnableTaskListener;
    }

    @Override
    public void onFailure(Exception e) {
        ExceptionsHelper.reThrowIfNotNull(e);
    }

    @Override
    public boolean isForceExecution() {
        return original instanceof AbstractRunnable && ((AbstractRunnable) original).isForceExecution();
    }

    @Override
    public void onRejection(final Exception e) {
        if (original instanceof AbstractRunnable) {
            ((AbstractRunnable) original).onRejection(e);
        } else {
            ExceptionsHelper.reThrowIfNotNull(e);
        }
    }

    @Override
    protected void doRun() throws Exception {
        assert runnableTaskListener.get() != null : "Listener should be attached";
        Long taskId = threadContext.getTransient(TASK_ID);
        if (Objects.nonNull(taskId)) {
            runnableTaskListener.get().taskExecutionStartedOnThread(taskId, currentThread().getId());
        } else {
            logger.debug("Task Id not available in thread context. Skipping update. Thread Info: {}", Thread.currentThread());
        }
        try {
            original.run();
        } finally {
            if (Objects.nonNull(taskId)) {
                runnableTaskListener.get().taskExecutionFinishedOnThread(taskId, currentThread().getId());
            }
        }
    }

    @Override
    public Runnable unwrap() {
        return original;
    }
}
