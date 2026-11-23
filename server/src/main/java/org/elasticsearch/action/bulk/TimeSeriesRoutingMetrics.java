/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.time.Instant;
import java.util.Map;

/**
 * Tracks TSDB timestamp routing failures on the coordinating node, regardless of how the document is handled after the failure.
 */
public final class TimeSeriesRoutingMetrics {

    public static final TimeSeriesRoutingMetrics NOOP = new TimeSeriesRoutingMetrics(MeterRegistry.NOOP);

    static final String TIMESTAMP_TOO_NEW = "es.data_stream.ingest.documents.time_series.timestamp_too_new.total";

    private static final String TIMESTAMP_IN_FUTURE = "es_data_stream_timestamp_in_future";
    private static final Map<String, Object> FUTURE_TIMESTAMP_ATTRIBUTES = Map.of(TIMESTAMP_IN_FUTURE, true);
    private static final Map<String, Object> NON_FUTURE_TIMESTAMP_ATTRIBUTES = Map.of(TIMESTAMP_IN_FUTURE, false);

    private final LongCounter timestampTooNew;

    public TimeSeriesRoutingMetrics(MeterRegistry meterRegistry) {
        timestampTooNew = meterRegistry.registerLongCounter(
            TIMESTAMP_TOO_NEW,
            "Number of documents whose timestamp was at or after the end of the latest TSDB backing index's time range",
            "documents"
        );
    }

    /**
     * Records whether a too-new timestamp is in the future according to the coordinating node's clock. A timestamp at or before
     * this time points to an end time that was not advanced in time.
     */
    void recordTooNewTimestamp(Instant timestamp, long nowMillis) {
        boolean inFuture = timestamp.isAfter(Instant.ofEpochMilli(nowMillis));
        timestampTooNew.incrementBy(1, inFuture ? FUTURE_TIMESTAMP_ATTRIBUTES : NON_FUTURE_TIMESTAMP_ATTRIBUTES);
    }
}
