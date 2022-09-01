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
 * natural ordering of the tasks, limiting the max number of concurrently running tasks.
 */
public class PrioritizedThrottledTaskRunner<T extends Comparable<T> & Runnable> {
    private static final Logger logger = LogManager.getLogger(PrioritizedThrottledTaskRunner.class);

    private final String taskRunnerName;
    private final int maxRunningTasks;
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
        pollAndSpawn();
    }

    // visible for testing
    protected void pollAndSpawn() {
        while (incrementRunningTasks()) {
            T task = tasks.poll();
            if (task == null) {
                logger.trace("[{}] task queue is empty", taskRunnerName);
                int decremented = runningTasks.decrementAndGet();
                assert decremented >= 0;
                if (tasks.peek() == null) break;
            } else {
                executor.execute(() -> runTask(task));
            }
        }
    }

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
            int decremented = runningTasks.decrementAndGet();
            assert decremented >= 0;
            pollAndSpawn();
        }
    }
}
