/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Generalization of thread utilization tracking. Tracks the total execution time of a set of threads and allows polling for the average
 * utilization since the last time the tracker was polled.
 */
public class ThreadUtilizationTracker {

    private long lastTotalExecutionTimeNanos = 0;
    private LongSupplier timeSupplierNanos;
    private long lastPollTimeNanos;
    private LongAdder executionTimeNanosAdder;
    private int numThreads = 0;

    /**
     * @param timeSupplierNanos Supplies the current time in nanoseconds. This is used to calculate the time elapsed since the last poll.
     * @param executionTimeNanosAdder Tracks the total execution time of the threads being monitored. Should be updated by those threads as
     *                                they execute tasks.
     * @param numThreads Sets the number of threads available for execution. This is necessary to calculate the maximum potential execution
     *                   time since the last poll, which is used as the denominator for calculating utilization.
     */
    public ThreadUtilizationTracker(LongSupplier timeSupplierNanos, LongAdder executionTimeNanosAdder, int numThreads) {
        assert numThreads > 0 : "expect the thread pool to have at least one thread";
        this.timeSupplierNanos = timeSupplierNanos;
        this.lastPollTimeNanos = timeSupplierNanos.getAsLong();
        this.executionTimeNanosAdder = executionTimeNanosAdder;
        this.numThreads = numThreads;
    }

    /**
     * Calculates the difference in total execution time since the last poll, and divides it by the maximum potential execution time (which
     * is the time since the last poll multiplied by the number of available execution threads).
     * <p>
     * Uses the LongAdder provided in the constructor to calculate the average thread utilization since the last time this method was
     * called. The LongAdder should be tracking the total execution time of the threads being monitored, and should be updated by those
     * threads as they execute tasks.
     * <p>
     * This method is synchronized to ensure that the state tracking variables are updated atomically with respect to the calculation.
     * @return the average thread utilization since the last time this method was called, as a value between 0 and 1 (inclusive)
     */
    public synchronized double pollUtilization() {
        final long currentTotalExecutionTimeNanos = executionTimeNanosAdder.sum();
        final long currentPollTimeNanos = timeSupplierNanos.getAsLong();

        final long totalExecutionTimeSinceLastPollNanos = currentTotalExecutionTimeNanos - lastTotalExecutionTimeNanos;
        final long timeSinceLastPoll = currentPollTimeNanos - lastPollTimeNanos;

        // Some sanity checks.
        assert currentPollTimeNanos >= lastPollTimeNanos : "currentPollTimeNanos must be greater than or equal to lastPollTimeNanos";
        assert currentTotalExecutionTimeNanos >= lastTotalExecutionTimeNanos
            : "currentTotalExecutionTimeNanos must be greater than or equal to lastTotalExecutionTimeNanos";

        if (timeSinceLastPoll == 0) {
            // If the time since the last poll is zero, we cannot calculate utilization.
            // Very unlikely in production, but avoiding 0 denominator as good practice.
            return 1;
        }

        final long maximumExecutionTimeSinceLastPollNanos = timeSinceLastPoll * numThreads;
        final double utilizationSinceLastPoll = (double) totalExecutionTimeSinceLastPollNanos / maximumExecutionTimeSinceLastPollNanos;

        lastTotalExecutionTimeNanos = currentTotalExecutionTimeNanos;
        lastPollTimeNanos = currentPollTimeNanos;

        return utilizationSinceLastPoll;
    }
}
