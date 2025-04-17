/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.ExceptionsHelper;

/**
 * A class used to wrap a {@code Runnable} that allows capturing the time of the task since creation
 * through execution as well as only execution time.
 */
class TimedRunnable extends AbstractRunnable implements WrappedRunnable {
    private final Runnable original;
    private final long creationTimeNanos;
    private long beforeExecuteTime = -1;
    private long startTimeNanos;
    private long finishTimeNanos = -1;
    private boolean failedOrRejected = false;

    TimedRunnable(final Runnable original) {
        this.original = original;
        this.creationTimeNanos = System.nanoTime();
    }

    @Override
    public void doRun() {
        try {
            startTimeNanos = System.nanoTime();
            original.run();
        } finally {
            finishTimeNanos = System.nanoTime();
        }
    }

    @Override
    public void onRejection(final Exception e) {
        this.failedOrRejected = true;
        if (original instanceof AbstractRunnable) {
            ((AbstractRunnable) original).onRejection(e);
        } else {
            ExceptionsHelper.reThrowIfNotNull(e);
        }
    }

    @Override
    public void onFailure(final Exception e) {
        this.failedOrRejected = true;
        ExceptionsHelper.reThrowIfNotNull(e);
    }

    @Override
    public boolean isForceExecution() {
        return original instanceof AbstractRunnable && ((AbstractRunnable) original).isForceExecution();
    }

    /**
     * Returns the time in nanoseconds between the creation time and the execution time
     *
     * @return The time in nanoseconds or -1 if the task was never de-queued
     */
    long getQueueTimeNanos() {
        if (beforeExecuteTime == -1) {
            assert false : "beforeExecute must be called before getQueueTimeNanos";
            return -1;
        }
        return beforeExecuteTime - creationTimeNanos;
    }

    /**
     * Return the time this task spent being run.
     * If the task is still running or has not yet been run, returns -1.
     */
    long getTotalExecutionNanos() {
        if (startTimeNanos == -1 || finishTimeNanos == -1) {
            // There must have been an exception thrown, the total time is unknown (-1)
            return -1;
        }
        return Math.max(finishTimeNanos - startTimeNanos, 1);
    }

    /**
     * Called when the task has reached the front of the queue and is about to be executed
     */
    public void beforeExecute() {
        beforeExecuteTime = System.nanoTime();
    }

    /**
     * If the task was failed or rejected, return true.
     * Otherwise, false.
     */
    boolean getFailedOrRejected() {
        return this.failedOrRejected;
    }

    @Override
    public Runnable unwrap() {
        return original;
    }

    @Override
    public String toString() {
        return "TimedRunnable{"
            + "original="
            + original
            + ", creationTimeNanos="
            + creationTimeNanos
            + ", startTimeNanos="
            + startTimeNanos
            + ", finishTimeNanos="
            + finishTimeNanos
            + ", failedOrRejected="
            + failedOrRejected
            + '}';
    }
}
