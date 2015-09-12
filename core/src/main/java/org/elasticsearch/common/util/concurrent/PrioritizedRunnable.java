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
package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public abstract class PrioritizedRunnable implements Runnable, Comparable<PrioritizedRunnable> {

    private final Priority priority;
    private final long creationDate;

    public static PrioritizedRunnable wrap(Runnable runnable, Priority priority) {
        return new Wrapped(runnable, priority);
    }

    protected PrioritizedRunnable(Priority priority) {
        this.priority = priority;
        creationDate = System.nanoTime();
    }

    public long getCreationDateInNanos() {
        return creationDate;
    }

    public long getAgeInMillis() {
        return Math.max(0, (System.nanoTime() - creationDate) / 1000);
    }

    @Override
    public int compareTo(PrioritizedRunnable pr) {
        return priority.compareTo(pr.priority);
    }

    public Priority priority() {
        return priority;
    }

    static class Wrapped extends PrioritizedRunnable {

        private final Runnable runnable;

        private Wrapped(Runnable runnable, Priority priority) {
            super(priority);
            this.runnable = runnable;
        }

        @Override
        public void run() {
            runnable.run();
        }
    }
}