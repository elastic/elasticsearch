/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class RepositoriesMetrics {

    public static RepositoriesMetrics NOOP = new RepositoriesMetrics(MeterRegistry.NOOP);

    private static final String METRIC_REQUESTS_COUNT = "es.repositories.requests.count";
    private static final String METRIC_EXCEPTIONS_COUNT = "es.repositories.exceptions.count";
    private static final String METRIC_THROTTLES_COUNT = "es.repositories.throttles.count";
    private static final String METRIC_OPERATIONS_COUNT = "es.repositories.operations.count";
    private static final String METRIC_UNSUCCESSFUL_OPERATIONS_COUNT = "es.repositories.operations.unsuccessful.count";
    private static final String METRIC_EXCEPTIONS_HISTOGRAM = "es.repositories.exceptions.histogram";
    private static final String METRIC_THROTTLES_HISTOGRAM = "es.repositories.throttles.histogram";

    private final LongCounter requestCounter;

    private final LongCounter exceptionCounter;
    private final LongCounter throttleCounter;
    private final LongCounter operationCounter;
    private final LongCounter unsuccessfulOperationCounter;
    private final LongHistogram exceptionHistogram;
    private final LongHistogram throttleHistogram;

    public RepositoriesMetrics(MeterRegistry meterRegistry) {
        this.requestCounter = meterRegistry.registerLongCounter(METRIC_REQUESTS_COUNT, "repository request counter", "unit");
        this.exceptionCounter = meterRegistry.registerLongCounter(METRIC_EXCEPTIONS_COUNT, "repository request exception counter", "unit");
        this.throttleCounter = meterRegistry.registerLongCounter(METRIC_THROTTLES_COUNT, "repository request throttle counter", "unit");
        this.operationCounter = meterRegistry.registerLongCounter(METRIC_OPERATIONS_COUNT, "repository operation counter", "unit");
        this.unsuccessfulOperationCounter = meterRegistry.registerLongCounter(
            METRIC_UNSUCCESSFUL_OPERATIONS_COUNT,
            "repository unsuccessful operation counter",
            "unit"
        );
        this.exceptionHistogram = meterRegistry.registerLongHistogram(
            METRIC_EXCEPTIONS_HISTOGRAM,
            "repository request exception histogram",
            "unit"
        );
        this.throttleHistogram = meterRegistry.registerLongHistogram(
            METRIC_THROTTLES_HISTOGRAM,
            "repository request throttle histogram",
            "unit"
        );
    }

    public LongCounter getRequestCounter() {
        return requestCounter;
    }

    public LongCounter getExceptionCounter() {
        return exceptionCounter;
    }

    public LongCounter getThrottleCounter() {
        return throttleCounter;
    }

    public LongCounter getOperationCounter() {
        return operationCounter;
    }

    public LongCounter getUnsuccessfulOperationCounter() {
        return unsuccessfulOperationCounter;
    }

    public LongHistogram getExceptionHistogram() {
        return exceptionHistogram;
    }

    public LongHistogram getThrottleHistogram() {
        return throttleHistogram;
    }
}
