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

public record RepositoriesMetrics(
    LongCounter requestCounter,
    LongCounter exceptionCounter,
    LongCounter throttleCounter,
    LongCounter operationCounter,
    LongCounter unsuccessfulOperationCounter,
    LongHistogram exceptionHistogram,
    LongHistogram throttleHistogram,
    LongHistogram httpRequestTimeInMicroHistogram
) {

    public static RepositoriesMetrics NOOP = new RepositoriesMetrics(MeterRegistry.NOOP);

    public static final String METRIC_REQUESTS_COUNT = "es.repositories.requests.count";
    public static final String METRIC_EXCEPTIONS_COUNT = "es.repositories.exceptions.count";
    public static final String METRIC_THROTTLES_COUNT = "es.repositories.throttles.count";
    public static final String METRIC_OPERATIONS_COUNT = "es.repositories.operations.count";
    public static final String METRIC_UNSUCCESSFUL_OPERATIONS_COUNT = "es.repositories.operations.unsuccessful.count";
    public static final String METRIC_EXCEPTIONS_HISTOGRAM = "es.repositories.exceptions.histogram";
    public static final String METRIC_THROTTLES_HISTOGRAM = "es.repositories.throttles.histogram";
    public static final String HTTP_REQUEST_TIME_IN_MICROS_HISTOGRAM = "es.repositories.requests.http_request_time.histogram";

    public RepositoriesMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongCounter(METRIC_REQUESTS_COUNT, "repository request counter", "unit"),
            meterRegistry.registerLongCounter(METRIC_EXCEPTIONS_COUNT, "repository request exception counter", "unit"),
            meterRegistry.registerLongCounter(METRIC_THROTTLES_COUNT, "repository request throttle counter", "unit"),
            meterRegistry.registerLongCounter(METRIC_OPERATIONS_COUNT, "repository operation counter", "unit"),
            meterRegistry.registerLongCounter(METRIC_UNSUCCESSFUL_OPERATIONS_COUNT, "repository unsuccessful operation counter", "unit"),
            meterRegistry.registerLongHistogram(METRIC_EXCEPTIONS_HISTOGRAM, "repository request exception histogram", "unit"),
            meterRegistry.registerLongHistogram(METRIC_THROTTLES_HISTOGRAM, "repository request throttle histogram", "unit"),
            meterRegistry.registerLongHistogram(
                HTTP_REQUEST_TIME_IN_MICROS_HISTOGRAM,
                "HttpRequestTime in microseconds expressed as as a histogram",
                "micros"
            )
        );
    }
}
