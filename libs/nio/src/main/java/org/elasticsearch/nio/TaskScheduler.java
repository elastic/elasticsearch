/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * A basic priority queue backed timer service. The service is thread local and should only be used by a
 * single nio selector event loop thread.
 */
public class TaskScheduler {

    private final PriorityQueue<DelayedTask> tasks = new PriorityQueue<>(Comparator.comparingLong(DelayedTask::getDeadline));

    /**
     * Schedule a task at the defined relative nanotime. When {@link #pollTask(long)} is called with a
     * relative nanotime after the scheduled time, the task will be returned. This method returns a
     * {@link Runnable} that can be run to cancel the scheduled task.
     *
     * @param task to schedule
     * @param relativeNanos defining when to execute the task
     * @return runnable that will cancel the task
     */
    public Runnable scheduleAtRelativeTime(Runnable task, long relativeNanos) {
        DelayedTask delayedTask = new DelayedTask(relativeNanos, task);
        tasks.offer(delayedTask);
        return delayedTask;
    }

    public Runnable pollTask(long relativeNanos) {
        DelayedTask task;
        while ((task = tasks.peek()) != null) {
            if (relativeNanos - task.deadline >= 0) {
                tasks.remove();
                if (task.cancelled == false) {
                    return task.runnable;
                }
            } else {
                return null;
            }
        }
        return null;
    }

    long nanosUntilNextTask(long relativeNanos) {
        DelayedTask nextTask = tasks.peek();
        if (nextTask == null) {
            return Long.MAX_VALUE;
        } else {
            return Math.max(nextTask.deadline - relativeNanos, 0);
        }
    }

    private static class DelayedTask implements Runnable {

        private final long deadline;
        private final Runnable runnable;
        private boolean cancelled = false;

        private DelayedTask(long deadline, Runnable runnable) {
            this.deadline = deadline;
            this.runnable = runnable;
        }

        private long getDeadline() {
            return deadline;
        }

        @Override
        public void run() {
            cancelled = true;
        }
    }
}
