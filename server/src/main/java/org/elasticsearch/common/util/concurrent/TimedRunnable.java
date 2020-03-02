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

import org.elasticsearch.ExceptionsHelper;

/**
 * A class used to wrap a {@code Runnable} that allows capturing the time of the task since creation
 * through execution as well as only execution time.
 */
class TimedRunnable extends AbstractRunnable implements WrappedRunnable {
    private final Runnable original;
    private final long creationTimeNanos;
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
     * Return the time since this task was created until it finished running.
     * If the task is still running or has not yet been run, returns -1.
     */
    long getTotalNanos() {
        if (finishTimeNanos == -1) {
            // There must have been an exception thrown, the total time is unknown (-1)
            return -1;
        }
        return Math.max(finishTimeNanos - creationTimeNanos, 1);
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
        return "TimedRunnable{" +
            "original=" + original +
            ", creationTimeNanos=" + creationTimeNanos +
            ", startTimeNanos=" + startTimeNanos +
            ", finishTimeNanos=" + finishTimeNanos +
            ", failedOrRejected=" + failedOrRejected +
            '}';
    }
}
