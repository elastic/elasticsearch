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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link PrioritizedThrottledTaskRunner} performs the enqueued tasks in the order dictated by the
 * natural ordering of the tasks, limiting the max number of concurrently running tasks. Each new task
 * that is dequeued to be run, is forked off to the given executor.
 */
public class PrioritizedThrottledTaskRunner<T extends Comparable<T> & Runnable> {
    private static final Logger logger = LogManager.getLogger(PrioritizedThrottledTaskRunner.class);

    private final String taskRunnerName;
    // The max number of tasks that this runner will schedule to concurrently run on the executor.
    private final int maxRunningTasks;
    // As we fork off dequeued tasks to the given executor, technically the following counter represents
    // the number of the concurrent pollAndSpawn calls currently checking the queue for a task to run. This
    // doesn't necessarily correspond to currently running tasks, since a pollAndSpawn could return without
    // actually running a task when the queue is empty.
    private final AtomicInteger runningTasks = new AtomicInteger();
    private final BlockingQueue<T> tasks = new PriorityBlockingQueue<>();
    private final Executor executor;

    public PrioritizedThrottledTaskRunner(final String name, final int maxRunningTasks, final Executor executor) {
        assert maxRunningTasks > 0;
        this.taskRunnerName = name;
        this.maxRunningTasks = maxRunningTasks;
        this.executor = executor;
    }

    public void enqueueTask(final T task) {
        logger.trace("[{}] enqueuing task {}", taskRunnerName, task);
        tasks.add(task);
        // Try to run a task since now there is at least one in the queue. If the maxRunningTasks is
        // reached, the task is just enqueued.
        pollAndSpawn();
    }

    // visible for testing
    protected void pollAndSpawn() {
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
                executor.execute(() -> runTask(task));
            }
        }
    }

    // Each worker thread that runs a task, first needs to get a "free slot" in order to respect maxRunningTasks.
    private boolean incrementRunningTasks() {
        int preUpdateValue = runningTasks.getAndUpdate(v -> v < maxRunningTasks ? v + 1 : v);
        assert preUpdateValue <= maxRunningTasks;
        return preUpdateValue < maxRunningTasks;
    }

    // Only use for testing
    public int runningTasks() {
        return runningTasks.get();
    }

    // Only use for testing
    public int queueSize() {
        return tasks.size();
    }

    private void runTask(final T task) {
        try {
            logger.trace("[{}] running task {}", taskRunnerName, task);
            task.run();
        } finally {
            // To avoid missing to run tasks that are enqueued and waiting, we check the queue again once running
            // a task is finished.
            int decremented = runningTasks.decrementAndGet();
            assert decremented >= 0;
            pollAndSpawn();
        }
    }
}
