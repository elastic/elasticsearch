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

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public abstract class PrioritizedRunnable implements Runnable, Comparable<PrioritizedRunnable> {

    private final Priority priority;
    private final long creationDate;
    private final LongSupplier relativeTimeProvider;

    public static WrappedRunnable wrap(Runnable runnable, Priority priority) {
        return new Wrapped(runnable, priority);
    }

    protected PrioritizedRunnable(Priority priority) {
        this(priority, System::nanoTime);
    }

    // package visible for testing
    PrioritizedRunnable(Priority priority, LongSupplier relativeTimeProvider) {
        this.priority = priority;
        this.creationDate = relativeTimeProvider.getAsLong();
        this.relativeTimeProvider = relativeTimeProvider;
    }

    public long getCreationDateInNanos() {
        return creationDate;
    }

    /**
     * The elapsed time in milliseconds since this instance was created,
     * as calculated by the difference between {@link System#nanoTime()}
     * at the time of creation, and {@link System#nanoTime()} at the
     * time of invocation of this method
     *
     * @return the age in milliseconds calculated
     */
    public long getAgeInMillis() {
        return TimeUnit.MILLISECONDS.convert(relativeTimeProvider.getAsLong() - creationDate, TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(PrioritizedRunnable pr) {
        return priority.compareTo(pr.priority);
    }

    public Priority priority() {
        return priority;
    }

    static class Wrapped extends PrioritizedRunnable implements WrappedRunnable {

        private final Runnable runnable;

        private Wrapped(Runnable runnable, Priority priority) {
            super(priority);
            this.runnable = runnable;
        }

        @Override
        public void run() {
            runnable.run();
        }

        @Override
        public Runnable unwrap() {
            return runnable;
        }

    }
}
