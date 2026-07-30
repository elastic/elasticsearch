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
import java.util.concurrent.PriorityBlockingQueue;

/**
 * {@link PrioritizedThrottledAsyncTaskRunner} performs the enqueued tasks in the order dictated by the
 * natural ordering of the tasks, limiting the max number of concurrently running tasks. Each new task
 * that is dequeued to be run, is forked off to the given executor.
 */
public class PrioritizedThrottledAsyncTaskRunner<T extends ActionListener<Releasable> & Comparable<T>> {
    private final AbstractThrottledTaskRunner<T> runner;
    private final PriorityBlockingQueue<T> queue;

    public PrioritizedThrottledAsyncTaskRunner(final String name, final int maxRunningTasks, final Executor executor) {
        this.queue = new PriorityBlockingQueue<>();
        this.runner = new AbstractThrottledTaskRunner<>(name, maxRunningTasks, executor, queue);
    }

    /**
     * Submits a task for execution. If there are fewer than {@code maxRunningTasks} tasks currently running then this task is immediately
     * submitted to the executor. Otherwise this task is enqueued and will be submitted to the executor in turn on completion of some other
     * task.
     */
    public void enqueueTask(final T task) {
        runner.enqueueTask(task);
    }

    // Only use for testing
    public int runningTasks() {
        return runner.runningTasks();
    }

    // Only use for testing
    public int queueSize() {
        return queue.size();
    }
}
