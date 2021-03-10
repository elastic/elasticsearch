/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
