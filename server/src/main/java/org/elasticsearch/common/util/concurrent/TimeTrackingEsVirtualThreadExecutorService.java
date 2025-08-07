/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.util.concurrent.EsExecutorService.TimeTrackingEsExecutorService;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.stream.Stream;

public class TimeTrackingEsVirtualThreadExecutorService extends EsVirtualThreadExecutorService implements TimeTrackingEsExecutorService {

    private final TaskTracker taskTracker;

    public TimeTrackingEsVirtualThreadExecutorService(
        String name,
        int maximumPoolSize,
        int maximumQueueSize,
        boolean rejectAfterShutdown,
        ThreadContext contextHolder,
        EsExecutors.TaskTrackingConfig trackingConfig
    ) {
        super(name, maximumPoolSize, maximumQueueSize, rejectAfterShutdown, contextHolder);
        this.taskTracker = new TaskTracker(trackingConfig, maximumPoolSize);
    }

    public Stream<Instrument> setupMetrics(MeterRegistry meterRegistry, String threadPoolName) {
        return Stream.concat(super.setupMetrics(meterRegistry, threadPoolName), taskTracker.setupMetrics(meterRegistry, threadPoolName));
    }

    @Override
    protected void doExecute(Runnable r, long creationTimeNanos) {
        taskTracker.trackTask(r);
        try {
            long startTimeNanos = System.nanoTime();
            taskTracker.taskQueueLatency(startTimeNanos - creationTimeNanos);
            super.doExecute(r, creationTimeNanos);
            taskTracker.taskExecutionTime(Math.max(System.nanoTime() - startTimeNanos, 1L));
        } finally {
            taskTracker.untrackTask(r);
        }
    }

    public long getMaxQueueLatencyMillisSinceLastPollAndReset() {
        return taskTracker.getMaxQueueLatencyMillisSinceLastPollAndReset();
    }

    /**
     * Returns the exponentially weighted moving average of the task execution time
     */
    public double getTaskExecutionEWMA() {
        return taskTracker.getTaskExecutionEWMA();
    }

    /**
     * Returns the fraction of the maximum possible thread time that was actually used since the last time this method was called.
     * There are two periodic pulling mechanisms that access utilization reporting: {@link UtilizationTrackingPurpose} distinguishes the
     * caller.
     *
     * @return the utilization as a fraction, in the range [0, 1]. This may return >1 if a task completed in the time range but started
     * earlier, contributing a larger execution time.
     */
    public double pollUtilization(UtilizationTrackingPurpose utilizationTrackingPurpose) {
        return taskTracker.pollUtilization(utilizationTrackingPurpose);
    }

    @Override
    protected void appendExecutorDetails(StringBuilder sb) {
        taskTracker.appendTaskExecutionDetails(sb);
    }
}
