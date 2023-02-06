/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link AbstractThrottledTaskRunner} runs the enqueued tasks using the given executor, limiting the number of tasks that are submitted to
 * the executor at once.
 */
public class AbstractThrottledTaskRunner<T extends ActionListener<Releasable>> {
    private static final Logger logger = LogManager.getLogger(AbstractThrottledTaskRunner.class);

    private final String taskRunnerName;
    // The max number of tasks that this runner will schedule to concurrently run on the executor.
    private final int maxRunningTasks;
    // As we fork off dequeued tasks to the given executor, technically the following counter represents
    // the number of the concurrent pollAndSpawn calls currently checking the queue for a task to run. This
    // doesn't necessarily correspond to currently running tasks, since a pollAndSpawn could return without
    // actually running a task when the queue is empty.
    private final AtomicInteger runningTasks = new AtomicInteger();
    private final Queue<T> tasks;
    private final Executor executor;

    public AbstractThrottledTaskRunner(final String name, final int maxRunningTasks, final Executor executor, final Queue<T> taskQueue) {
        assert maxRunningTasks > 0;
        this.taskRunnerName = name;
        this.maxRunningTasks = maxRunningTasks;
        this.executor = executor;
        this.tasks = taskQueue;
    }

    /**
     * Submits a task for execution. If there are fewer than {@code maxRunningTasks} tasks currently running then this task is immediately
     * submitted to the executor. Otherwise this task is enqueued and will be submitted to the executor in turn on completion of some other
     * task.
     *
     * Tasks are executed via their {@link ActionListener#onResponse} method, receiving a {@link Releasable} which must be closed on
     * completion of the task. Task which are rejected from their executor are notified via their {@link ActionListener#onFailure} method.
     * Neither of these methods may themselves throw exceptions.
     */
    public void enqueueTask(final T task) {
        logger.trace("[{}] enqueuing task {}", taskRunnerName, task);
        tasks.add(task);
        // Try to run a task since now there is at least one in the queue. If the maxRunningTasks is
        // reached, the task is just enqueued.
        pollAndSpawn();
    }

    /**
     * Allows certain tasks to force their execution, bypassing the queue-length limit on the executor. See also {@link
     * AbstractRunnable#isForceExecution()}.
     */
    protected boolean isForceExecution(@SuppressWarnings("unused") /* TODO test this */ T task) {
        return false;
    }

    private void pollAndSpawn() {
        // A pollAndSpawn attempts to run a new task. There could be many concurrent pollAndSpawn calls competing
        // to get a "free slot", since we attempt to run a new task on every enqueueTask call and every time an
        // existing task is finished.
        while (incrementRunningTasks()) {
            T task = tasks.poll();
            if (task == null) {
                logger.trace("[{}] task queue is empty", taskRunnerName);
                // We have taken up a "free slot", but there are no tasks in the queue! This could happen each time a worker
                // sees an empty queue after running a task. Decrement to give competing pollAndSpawn calls a chance!
                int decremented = runningTasks.decrementAndGet();
                assert decremented >= 0;
                // We might have blocked all competing pollAndSpawn calls. This could happen for example when
                // maxRunningTasks=1 and a task got enqueued just after checking the queue but before decrementing.
                // To be sure, return only if the queue is still empty. If the queue is not empty, this might be the
                // only pollAndSpawn call in progress, and returning without peeking would risk ending up with a
                // non-empty queue and no workers!
                if (tasks.peek() == null) break;
            } else {
                final boolean isForceExecution = isForceExecution(task);
                executor.execute(new AbstractRunnable() {
                    private boolean rejected; // need not be volatile - if we're rejected then that happens-before calling onAfter

                    private final Releasable releasable = Releasables.releaseOnce(() -> {
                        // To avoid missing to run tasks that are enqueued and waiting, we check the queue again once running
                        // a task is finished.
                        int decremented = runningTasks.decrementAndGet();
                        assert decremented >= 0;

                        if (rejected == false) {
                            pollAndSpawn();
                        }
                    });

                    @Override
                    public boolean isForceExecution() {
                        return isForceExecution;
                    }

                    @Override
                    public void onRejection(Exception e) {
                        logger.trace("[{}] task {} rejected", taskRunnerName, task);
                        rejected = true;
                        try {
                            task.onFailure(e);
                        } finally {
                            releasable.close();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // should not happen
                        logger.error(() -> Strings.format("[%s] task %s failed", taskRunnerName, task), e);
                        assert false : e;
                        task.onFailure(e);
                    }

                    @Override
                    protected void doRun() {
                        logger.trace("[{}] running task {}", taskRunnerName, task);
                        task.onResponse(releasable);
                    }

                    @Override
                    public String toString() {
                        return task.toString();
                    }
                });
            }
        }
    }

    // Each worker thread that runs a task, first needs to get a "free slot" in order to respect maxRunningTasks.
    private boolean incrementRunningTasks() {
        int preUpdateValue = runningTasks.getAndAccumulate(maxRunningTasks, (v, maxRunning) -> v < maxRunning ? v + 1 : v);
        assert preUpdateValue <= maxRunningTasks;
        return preUpdateValue < maxRunningTasks;
    }

    // exposed for testing
    int runningTasks() {
        return runningTasks.get();
    }

}
