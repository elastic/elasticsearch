/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.Executor;

import static org.elasticsearch.common.Strings.format;

public class ThrottledTaskRunner extends AbstractThrottledTaskRunner<ActionListener<Releasable>> {
    // a simple AbstractThrottledTaskRunner which fixes the task type and uses a regular FIFO blocking queue.
    public ThrottledTaskRunner(String name, int maxRunningTasks, Executor executor) {
        super(name, maxRunningTasks, executor, ConcurrentCollections.newBlockingQueue());
    }

    /**
     * Returns a new {@link Executor} implementation that delegates tasks to this {@link ThrottledTaskRunner}.
     * <br>
     * NOTE: The {@link Runnable}s to be executed via {@link Executor#execute(Runnable)} are throttled to the extent that
     * they do NOT fork off on a different executor. If they do, the part that's forked off is not throttled by this
     * {@link ThrottledTaskRunner}. If more control is needed for when a task is done for throttling purposes,
     * see {@link AbstractThrottledTaskRunner#enqueueTask(ActionListener)}.
     */
    public Executor asExecutor() {
        return new ThrottledExecutorAdapter(this);
    }

    /**
     * Adapter from the {@link AbstractThrottledTaskRunner} interface to the {@link Executor} one.
     */
    static class ThrottledExecutorAdapter implements Executor {
        private final org.elasticsearch.logging.Logger logger = org.elasticsearch.logging.LogManager.getLogger(
            ThrottledExecutorAdapter.class
        );

        private final ThrottledTaskRunner throttledTaskRunner;

        /**
         * Be extra careful that any forked off work is NOT throttled by the passed-in {@param throttledTaskRunner}.
         */
        ThrottledExecutorAdapter(ThrottledTaskRunner throttledTaskRunner) {
            this.throttledTaskRunner = throttledTaskRunner;
        }

        @Override
        public void execute(Runnable task) {
            throttledTaskRunner.enqueueTask(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    try (releasable) {
                        // if this forks, the {@code releasable} is closed, and any waiting (throttled) tasks can be executed
                        task.run();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(
                        () -> format(
                            "[%s] failed to execute task %s by executor [%s]",
                            throttledTaskRunner.getTaskRunnerName(),
                            task,
                            ThrottledExecutorAdapter.class.getCanonicalName()
                        ),
                        e
                    );
                }

                @Override
                public String toString() {
                    return task.toString();
                }
            });
        }
    }
}
