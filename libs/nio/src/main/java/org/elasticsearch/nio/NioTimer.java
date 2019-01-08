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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

class NioTimer {

    private final PriorityQueue<DelayedTask> tasks;

    NioTimer() {
        tasks = new PriorityQueue<>(Comparator.comparingLong(DelayedTask::getDeadline));
    }

    Runnable schedule(Runnable task, TimeValue timeValue) {
        return scheduleAtRelativeTime(task, System.nanoTime() + timeValue.nanos());
    }

    Runnable scheduleAtRelativeTime(Runnable task, long relativeNanos) {
        DelayedTask delayedTask = new DelayedTask(relativeNanos, task);
        tasks.offer(delayedTask);
        return delayedTask;
    }

    Runnable pollTask() {
        return pollTask(System.nanoTime());
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
