/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public interface EsExecutorService extends ExecutorService {
    long getCompletedTaskCount();

    int getActiveCount();

    int getCurrentQueueSize();

    int getMaximumPoolSize();

    int getPoolSize();

    int getLargestPoolSize();

    // FIXME is this required?! Any usage outside of tests? Investigate InternalWatchExecutor?
    default Stream<Runnable> getTasks() {
        return Stream.empty();
    }

    default Stream<Instrument> setupMetrics(MeterRegistry meterRegistry, String threadPoolName) {
        return Stream.empty();
    };

    interface ExecutionTimeTrackingEsExecutorService extends EsExecutorService {
        /**
         * Returns the exponentially weighted moving average of the task execution time
         */
        double getTaskExecutionEWMA();

        long getMaxQueueLatencyMillisSinceLastPollAndReset();

        enum UtilizationTrackingPurpose {
            APM,
            ALLOCATION,
        }

        /**
         * Returns the fraction of the maximum possible thread time that was actually used since the last time this method was called.
         * There are two periodic pulling mechanisms that access utilization reporting: {@link TaskExecutionTimeTrackingEsThreadPoolExecutor.UtilizationTrackingPurpose} distinguishes the
         * caller.
         *
         * @return the utilization as a fraction, in the range [0, 1]. This may return >1 if a task completed in the time range but started
         * earlier, contributing a larger execution time.
         */
        double pollUtilization(UtilizationTrackingPurpose utilizationTrackingPurpose);
    }
}
