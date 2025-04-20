/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/**
 * The common set of metrics that we publish for {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository} implementations.
 */
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

    public static final RepositoriesMetrics NOOP = new RepositoriesMetrics(MeterRegistry.NOOP);

    /**
     * Is incremented for each request sent to the blob store (including retries)
     *
     * Exposed as {@link #requestCounter()}
     */
    public static final String METRIC_REQUESTS_TOTAL = "es.repositories.requests.total";
    /**
     * Is incremented for each request which returns a non <code>2xx</code> response OR fails to return a response
     * (includes throttling and retryable errors)
     *
     * Exposed as {@link #exceptionCounter()}
     */
    public static final String METRIC_EXCEPTIONS_TOTAL = "es.repositories.exceptions.total";
    /**
     * Is incremented each time an operation ends with a <code>416</code> response
     *
     * Exposed as {@link #requestRangeNotSatisfiedExceptionCounter()}
     */
    public static final String METRIC_EXCEPTIONS_REQUEST_RANGE_NOT_SATISFIED_TOTAL =
        "es.repositories.exceptions.request_range_not_satisfied.total";
    /**
     * Is incremented each time we are throttled by the blob store, e.g. upon receiving an HTTP <code>429</code> response
     *
     * Exposed as {@link #throttleCounter()}
     */
    public static final String METRIC_THROTTLES_TOTAL = "es.repositories.throttles.total";
    /**
     * Is incremented for each operation we attempt, whether it succeeds or fails, this doesn't include retries
     *
     * Exposed via {@link #operationCounter()}
     */
    public static final String METRIC_OPERATIONS_TOTAL = "es.repositories.operations.total";
    /**
     * Is incremented for each operation that ends with a non <code>2xx</code> response or throws an exception
     *
     * Exposed via {@link #unsuccessfulOperationCounter()}
     */
    public static final String METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL = "es.repositories.operations.unsuccessful.total";
    /**
     * Each time an operation has one or more failed requests (from non <code>2xx</code> response or exception), the
     * count of those is sampled
     *
     * Exposed via {@link #exceptionHistogram()}
     */
    public static final String METRIC_EXCEPTIONS_HISTOGRAM = "es.repositories.exceptions.histogram";
    /**
     * Each time an operation has one or more throttled requests, the count of those is sampled
     *
     * Exposed via {@link #throttleHistogram()}
     */
    public static final String METRIC_THROTTLES_HISTOGRAM = "es.repositories.throttles.histogram";
    /**
     * Every operation that is attempted will record a time. The value recorded here is the sum of the duration of
     * each of the requests executed to try and complete the operation. The duration of each request is the time
     * between sending the request and either a response being received, or the request failing. Does not include
     * the consumption of the body of the response or any time spent pausing between retries.
     *
     * Exposed via {@link #httpRequestTimeInMillisHistogram()}
     */
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

    /**
     * Create the map of attributes we expect to see on repository metrics
     */
    public static Map<String, Object> createAttributesMap(RepositoryMetadata meta, OperationPurpose purpose, String operation) {
        return Map.of("repo_type", meta.type(), "repo_name", meta.name(), "operation", operation, "purpose", purpose.getKey());
    }

}
