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

public class NioTimer {

    private final PriorityQueue<DelayedTask> tasks;

    public NioTimer() {
        tasks = new PriorityQueue<>(Comparator.comparingLong(value -> value.deadline));
    }

    public void schedule(Runnable task, TimeValue timeValue) {
        long nanos = timeValue.getNanos();
        long currentTime = System.nanoTime();


    }

    public void scheduleAtRelativeTime(Runnable task, long relativeTime) {
        tasks.offer(new DelayedTask(relativeTime, task));
    }

    public void pollTasks() {
        long currentNanos = System.nanoTime();

    }

    private static class DelayedTask {

        private final long deadline;
        private final Runnable runnable;

        private DelayedTask(long deadline, Runnable runnable) {
            this.deadline = deadline;
            this.runnable = runnable;
        }
    }
}
