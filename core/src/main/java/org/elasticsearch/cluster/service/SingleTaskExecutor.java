/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SingleTaskExecutor {

    protected final Logger logger;
    protected final ThreadPool threadPool;
    protected final PrioritizedEsThreadPoolExecutor threadExecutor;

    protected SingleTaskExecutor(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor, ThreadPool threadPool) {
        this.logger = logger;
        this.threadExecutor = threadExecutor;
        this.threadPool = threadPool;
    }

    /**
     * Submits a task
     */
    public void submitTask(SingleTask task, @Nullable TimeValue timeout) throws EsRejectedExecutionException {
        if (timeout != null) {
            threadExecutor.execute(task, threadPool.scheduler(), timeout, () -> onTimeoutInternal(task, timeout));
        } else {
            threadExecutor.execute(task);
        }
    }

    /**
     * Submits a runnable to be directly executed on the underlying {@link PrioritizedEsThreadPoolExecutor}
     */
    public void submitRaw(SourcePrioritizedRunnable runnable) {
        threadExecutor.execute(runnable);
    }

    private void runIfNotProcessed(SingleTask task) {
        if (task.processed.getAndSet(true) == false) {
            logger.trace("will process {}", task.source);
            run(task);
        } else {
            logger.trace("skipping {}, already processed", task.source);
        }
    }

    private void onTimeoutInternal(SingleTask task, TimeValue timeout) {
        threadPool.generic().execute(() -> {
            if (task.processed.getAndSet(true) == false) {
                logger.debug("task [{}] timed out after [{}]", task.source, timeout);
                onTimeout(task, timeout);
            }
        });
    }

    /**
     * Action to be implemented by the specific task executor
     *
     * @param task the task to execute
     */
    protected abstract void run(SingleTask task);

    /**
     * Action to be implemented by the specific task executor
     *
     * @param task the task that timed out
     * @param timeout the timeout value
     */
    protected abstract void onTimeout(SingleTask task, TimeValue timeout);

    /**
     * Returns the tasks that are pending.
     */
    public List<PendingClusterTask> pendingTasks() {
        PrioritizedEsThreadPoolExecutor.Pending[] pendings = threadExecutor.getPending();
        List<PendingClusterTask> pendingClusterTasks = new ArrayList<>(pendings.length);
        for (PrioritizedEsThreadPoolExecutor.Pending pending : pendings) {
            final String source;
            final long timeInQueue;
            // we have to capture the task as it will be nulled after execution and we don't want to change while we check things here.
            final Object task = pending.task;
            if (task == null) {
                continue;
            } else if (task instanceof SourcePrioritizedRunnable) {
                SourcePrioritizedRunnable runnable = (SourcePrioritizedRunnable) task;
                source = runnable.source();
                timeInQueue = runnable.getAgeInMillis();
            } else {
                assert false : "expected SourcePrioritizedRunnable got " + task.getClass();
                source = "unknown [" + task.getClass() + "]";
                timeInQueue = 0;
            }

            pendingClusterTasks.add(
                new PendingClusterTask(pending.insertionOrder, pending.priority, new Text(source), timeInQueue, pending.executing));
        }
        return pendingClusterTasks;
    }

    /**
     * Returns the number of currently pending tasks.
     */
    public int numberOfPendingTasks() {
        return threadExecutor.getNumberOfPendingTasks();
    }

    /**
     * Returns the maximum wait time for tasks in the queue
     *
     * @return A zero time value if the queue is empty, otherwise the time value oldest task waiting in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        return threadExecutor.getMaxTaskWaitTime();
    }

    public abstract static class SourcePrioritizedRunnable extends PrioritizedRunnable {
        protected final String source;

        public SourcePrioritizedRunnable(Priority priority, String source) {
            super(priority);
            this.source = source;
        }

        public String source() {
            return source;
        }
    }

    /**
     * Represents a runnable task. Implementors of SingleTaskExecutor can subclass this to add a payload to the task.
     */
    protected class SingleTask extends SourcePrioritizedRunnable {
        protected final AtomicBoolean processed = new AtomicBoolean();

        protected SingleTask(Priority priority, String source) {
            super(priority, source);
        }

        @Override
        public void run() {
            runIfNotProcessed(this);
        }

        @Override
        public String toString() {
            return "[" + source + "]";
        }
    }
}
