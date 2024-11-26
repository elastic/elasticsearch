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

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class EsRejectedExecutionHandler implements RejectedExecutionHandler {

    private final CounterMetric rejected = new CounterMetric();
    private LongCounter rejectionCounter = null;

    /**
     * The number of rejected executions.
     */
    public long rejected() {
        return rejected.count();
    }

    protected void incrementRejections() {
        rejected.inc();
        if (rejectionCounter != null) {
            rejectionCounter.increment();
        }
    }

    public void registerCounter(MeterRegistry meterRegistry, String metric_name, String threadpool_name) {
        rejectionCounter = meterRegistry.registerLongCounter(metric_name, "number of rejected threads for " + threadpool_name, "count");
        rejectionCounter.incrementBy(rejected());
    }

    protected static EsRejectedExecutionException newRejectedException(
        Runnable r,
        ThreadPoolExecutor executor,
        boolean isExecutorShutdown
    ) {
        final StringBuilder builder = new StringBuilder("rejected execution of ").append(r).append(" on ").append(executor);
        if (isExecutorShutdown) {
            builder.append(" (shutdown)");
        }
        return new EsRejectedExecutionException(builder.toString(), isExecutorShutdown);
    }
}
