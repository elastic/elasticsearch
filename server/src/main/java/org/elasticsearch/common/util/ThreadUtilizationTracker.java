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

/**
 * Generalization of thread utilization tracking. Tracks the total execution time of a set of threads and allows polling for the average
 * utilization since the last time the tracker was polled.
 */
public class ThreadUtilizationTracker {

    private long lastTotalExecutionTime = 0;
    private long lastPollTime = System.nanoTime();
    private int numThreads = 0;

    /**
     * Set the number of threads available for execution. This is necessary to calculate the maximum potential execution time since the last
     * poll, which is used as the denominator for calculating utilization.
     */
    public ThreadUtilizationTracker(int numThreads) {
        this.numThreads = numThreads;
    }

    /**
     * Calculates the difference in total execution time since the last poll, and divides it by the maximum potential execution time (which
     * is the time since the last poll multiplied by the number of available execution threads).
     * <p>
     * Uses the provided LongAdder to calculate the average thread utilization since the last time this method was called. The LongAdder
     * should be tracking the total execution time of the threads being monitored, and should be updated by those threads as they execute
     * tasks.
     * <p>
     * This method is synchronized to ensure that the state tracking variables are updated atomically with respect to the calculation.
     * @return the average thread utilization since the last time this method was called, as a value between 0 and 1 (inclusive)
     */
    public synchronized double pollUtilization(LongAdder executionTimeAdder) {
        final long currentTotalExecutionTimeNanos = executionTimeAdder.sum();
        final long currentPollTimeNanos = System.nanoTime();

        final long totalExecutionTimeSinceLastPollNanos = currentTotalExecutionTimeNanos - lastTotalExecutionTime;
        final long timeSinceLastPoll = currentPollTimeNanos - lastPollTime;

        final long maximumExecutionTimeSinceLastPollNanos = timeSinceLastPoll * numThreads;
        final double utilizationSinceLastPoll = (double) totalExecutionTimeSinceLastPollNanos / maximumExecutionTimeSinceLastPollNanos;

        lastTotalExecutionTime = currentTotalExecutionTimeNanos;
        lastPollTime = currentPollTimeNanos;

        return utilizationSinceLastPoll;
    }
}
