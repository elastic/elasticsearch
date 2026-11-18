/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;

import java.util.Map;

/**
 * APM metrics for flushes. The failure count is for internal alerting on unexpected flush failures, so it is not part of the public
 * stats API.
 */
public class FlushMetrics {

    public static final String FLUSH_FAILURE_TOTAL = "es.flush.failure.total";
    public static final String FLUSH_FAILURE_BLOBSTORE_TRANSIENT_TOTAL = "es.flush.failure.blobstore_transient.total";
    public static final FlushMetrics NOOP = new FlushMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    private final LongCounter flushFailures;
    private final LongCounter flushBlobStoreTransientFailures;

    public FlushMetrics(MeterRegistry meterRegistry) {
        flushFailures = meterRegistry.registerLongCounter(
            FLUSH_FAILURE_TOTAL,
            "Number of flushes that failed for a reason not expected during normal operation; "
                + "flushes rejected due to a cleanly closing shard or engine, or a pending translog recovery, are not counted",
            "unit"
        );
        flushBlobStoreTransientFailures = meterRegistry.registerLongCounter(
            FLUSH_FAILURE_BLOBSTORE_TRANSIENT_TOTAL,
            "Number of flushes that failed because the blob store was unreachable or closed; also counted in " + FLUSH_FAILURE_TOTAL,
            "unit"
        );
    }

    public void onFailure(IndexMode indexMode, Throwable error) {
        // the engine wraps every commit or refresh failure, so report the cause to tell those failures apart
        final Throwable cause = error instanceof FlushFailedEngineException && error.getCause() != null ? error.getCause() : error;
        final Map<String, Object> attributes = Map.of(
            MetricAttributes.ES_INDEX_MODE,
            indexMode.getName(),
            MetricAttributes.ERROR_TYPE,
            MetricAttributes.errorType(cause)
        );
        flushFailures.incrementBy(1, attributes);
        if (MergeMetrics.isBlobStoreClientFailure(error)) {
            flushBlobStoreTransientFailures.incrementBy(1, attributes);
        }
    }
}
