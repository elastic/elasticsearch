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
    MeterRegistry meterRegistry,
    LongCounter requestCounter,
    LongCounter exceptionCounter,
    LongCounter requestRangeNotSatisfiedExceptionCounter,
    LongCounter throttleCounter,
    LongCounter operationCounter,
    LongCounter unsuccessfulOperationCounter,
    LongHistogram exceptionHistogram,
    LongHistogram throttleHistogram,
    LongHistogram httpRequestTimeInMillisHistogram
) {

    public static RepositoriesMetrics NOOP = new RepositoriesMetrics(MeterRegistry.NOOP);

    public static final String METRIC_REQUESTS_TOTAL = "es.repositories.requests.total";
    public static final String METRIC_EXCEPTIONS_TOTAL = "es.repositories.exceptions.total";
    public static final String METRIC_EXCEPTIONS_REQUEST_RANGE_NOT_SATISFIED_TOTAL =
        "es.repositories.exceptions.request_range_not_satisfied.total";
    public static final String METRIC_THROTTLES_TOTAL = "es.repositories.throttles.total";
    public static final String METRIC_OPERATIONS_TOTAL = "es.repositories.operations.total";
    public static final String METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL = "es.repositories.operations.unsuccessful.total";
    public static final String METRIC_EXCEPTIONS_HISTOGRAM = "es.repositories.exceptions.histogram";
    public static final String METRIC_THROTTLES_HISTOGRAM = "es.repositories.throttles.histogram";
    public static final String HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM = "es.repositories.requests.http_request_time.histogram";

    public RepositoriesMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry,
            meterRegistry.registerLongCounter(METRIC_REQUESTS_TOTAL, "repository request counter", "unit"),
            meterRegistry.registerLongCounter(METRIC_EXCEPTIONS_TOTAL, "repository request exception counter", "unit"),
            meterRegistry.registerLongCounter(
                METRIC_EXCEPTIONS_REQUEST_RANGE_NOT_SATISFIED_TOTAL,
                "repository request RequestedRangeNotSatisfiedException counter",
                "unit"
            ),
            meterRegistry.registerLongCounter(METRIC_THROTTLES_TOTAL, "repository request throttle counter", "unit"),
            meterRegistry.registerLongCounter(METRIC_OPERATIONS_TOTAL, "repository operation counter", "unit"),
            meterRegistry.registerLongCounter(METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL, "repository unsuccessful operation counter", "unit"),
            meterRegistry.registerLongHistogram(METRIC_EXCEPTIONS_HISTOGRAM, "repository request exception histogram", "unit"),
            meterRegistry.registerLongHistogram(METRIC_THROTTLES_HISTOGRAM, "repository request throttle histogram", "unit"),
            meterRegistry.registerLongHistogram(
                HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM,
                "HttpRequestTime in milliseconds expressed as as a histogram",
                "ms"
            )
        );
    }
}
