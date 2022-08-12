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
 * ThrottledTaskRunner performs the enqueued tasks in the order dictated by the natural ordering
 * of the tasks, limiting the max number of concurrently running tasks.
 */
public class ThrottledTaskRunner<T extends Comparable<T> & Runnable> {
    private static final Logger logger = LogManager.getLogger(ThrottledTaskRunner.class);

    private final int maxRunningTasks;
    private final AtomicInteger runningTasks = new AtomicInteger();
    private final BlockingQueue<T> tasks = new PriorityBlockingQueue<>();
    private final Executor executor;

    public ThrottledTaskRunner(final int maxRunningTasks, final Executor executor) {
        assert maxRunningTasks > 0;
        this.maxRunningTasks = maxRunningTasks;
        this.executor = executor;
    }

    public void enqueueTask(final T task) {
        logger.trace("enqueuing task {}", task);
        tasks.add(task);
        pollAndSpawn();
    }

    private void pollAndSpawn() {
        while (incrementRunningTasks()) {
            T task = tasks.poll();
            if (task == null) {
                logger.trace("task queue is empty");
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

    public int runningTasks() {
        return runningTasks.get();
    }

    public int queueSize() {
        return tasks.size();
    }

    private void runTask(final T task) {
        try {
            logger.trace("running task {}", task);
            task.run();
        } finally {
            int decremented = runningTasks.decrementAndGet();
            assert decremented >= 0;
            pollAndSpawn();
        }
    }
}
