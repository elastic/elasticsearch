/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;

import static org.elasticsearch.threadpool.ThreadPool.THREAD_POOL_METRIC_NAME_REJECTED;
import static org.elasticsearch.threadpool.ThreadPool.THREAD_POOL_METRIC_PREFIX;

public abstract class EsRejectedExecutionHandler implements RejectedExecutionHandler {

    static class RejectionMetrics {
        private final CounterMetric rejected = new CounterMetric();
        private LongCounter rejectionCounter = null;

        void incrementRejections() {
            rejected.inc();
            if (rejectionCounter != null) {
                rejectionCounter.increment();
            }
        }

        void registerCounter(MeterRegistry meterRegistry, String threadPoolName) {
            rejectionCounter = meterRegistry.registerLongCounter(
                THREAD_POOL_METRIC_PREFIX + threadPoolName + THREAD_POOL_METRIC_NAME_REJECTED,
                "number of rejected threads for " + threadPoolName,
                "count"
            );
            rejectionCounter.incrementBy(getRejectedTaskCount());
        }

        long getRejectedTaskCount() {
            return rejected.count();
        }
    }

    private final RejectionMetrics rejectionMetrics = new RejectionMetrics();

    /**
     * The number of rejected executions.
     */
    public long getRejectedTaskCount() {
        return rejectionMetrics.getRejectedTaskCount();
    }

    protected void incrementRejections() {
        rejectionMetrics.incrementRejections();
    }

    public void registerCounter(MeterRegistry meterRegistry, String threadPoolName) {
        rejectionMetrics.registerCounter(meterRegistry, threadPoolName);
    }

    protected static EsRejectedExecutionException newRejectedException(Runnable r, ExecutorService executor, boolean isExecutorShutdown) {
        final StringBuilder builder = new StringBuilder("rejected execution of ").append(r).append(" on ").append(executor);
        if (isExecutorShutdown) {
            builder.append(" (shutdown)");
        }
        return new EsRejectedExecutionException(builder.toString(), isExecutorShutdown);
    }
}
