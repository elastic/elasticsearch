/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Objects;

/**
 * Project-level forecast outcome counters, with no job or forecast attributes. A hard node death or a process
 * that ends before reporting any stats never emits finished or failed, so requested can permanently exceed their sum.
 * A stopped forecast is counted failed only when the process ends.
 */
public final class ForecastTelemetry {

    public static final ForecastTelemetry NOOP = new ForecastTelemetry(MeterRegistry.NOOP);

    public static final String REQUESTED_METRIC = "es.ml.forecasts.requested.total";
    public static final String FINISHED_METRIC = "es.ml.forecasts.finished.total";
    public static final String FAILED_METRIC = "es.ml.forecasts.failed.total";

    private final LongCounter requestedCounter;
    private final LongCounter finishedCounter;
    private final LongCounter failedCounter;

    public ForecastTelemetry(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry);
        this.requestedCounter = meterRegistry.registerLongCounter(
            REQUESTED_METRIC,
            "Count of forecast requests accepted for processing by an anomaly detection job.",
            "forecasts"
        );
        this.finishedCounter = meterRegistry.registerLongCounter(
            FINISHED_METRIC,
            "Count of forecasts that reached the finished state.",
            "forecasts"
        );
        this.failedCounter = meterRegistry.registerLongCounter(
            FAILED_METRIC,
            "Count of accepted forecasts that failed, including those aborted by the autodetect process ending.",
            "forecasts"
        );
    }

    public void recordRequested() {
        requestedCounter.increment();
    }

    public void recordFinished() {
        finishedCounter.increment();
    }

    public void recordFailed() {
        failedCounter.increment();
    }
}
