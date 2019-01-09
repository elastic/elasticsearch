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

package org.elasticsearch.nio;

import org.elasticsearch.common.unit.TimeValue;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * A basic priority queue backed timer service. The service is thread local and should only be used by a
 * single nio selector event loop thread.
 */
public class NioTimer {

    private final PriorityQueue<DelayedTask> tasks = new PriorityQueue<>(Comparator.comparingLong(DelayedTask::getDeadline));

    public Cancellable schedule(Runnable task, TimeValue timeValue) {
        return scheduleAtRelativeTime(task, System.nanoTime() + timeValue.nanos());
    }

    public Cancellable scheduleAtRelativeTime(Runnable task, long relativeNanos) {
        DelayedTask delayedTask = new DelayedTask(relativeNanos, task);
        tasks.offer(delayedTask);
        return delayedTask;
    }

    Runnable pollTask(long relativeNanos) {
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

    public interface Cancellable {

        void cancel();
    }

    private static class DelayedTask implements Cancellable {

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
        public void cancel() {
            cancelled = true;
        }
    }
}
