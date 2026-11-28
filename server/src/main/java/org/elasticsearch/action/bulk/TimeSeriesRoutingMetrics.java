/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.time.Instant;
import java.util.Map;

/**
 * Tracks TSDB timestamp routing failures on the coordinating node, regardless of how the document is handled after the failure.
 */
public final class TimeSeriesRoutingMetrics {

    public static final TimeSeriesRoutingMetrics NOOP = new TimeSeriesRoutingMetrics(MeterRegistry.NOOP);

    static final String TIMESTAMP_OUT_OF_BOUNDS = "es.data_stream.ingest.documents.time_series.timestamp_out_of_bounds.total";

    private static final String OUT_OF_BOUNDS_REASON = "es_data_stream_timestamp_out_of_bounds_reason";

    private final LongCounter timestampOutOfBounds;

    public TimeSeriesRoutingMetrics(MeterRegistry meterRegistry) {
        timestampOutOfBounds = meterRegistry.registerLongCounter(
            TIMESTAMP_OUT_OF_BOUNDS,
            "Number of documents whose timestamp did not match any writable TSDB backing index time range",
            "documents"
        );
    }

    /**
     * Records where the timestamp fell outside the writable ranges. Beyond the end time, the past bucket includes timestamps equal
     * to the coordinating node's current time.
     */
    void recordTimestampOutOfBounds(DataStream.TimestampOutOfBounds outOfBounds, long nowMillis) {
        String reason = switch (outOfBounds.position()) {
            case BEFORE_EARLIEST_RANGE -> "before_earliest_range";
            case BETWEEN_RANGES -> "between_ranges";
            case BEYOND_END_TIME -> outOfBounds.timestamp().isAfter(Instant.ofEpochMilli(nowMillis))
                ? "beyond_end_time_in_future"
                : "beyond_end_time_in_past";
        };
        timestampOutOfBounds.incrementBy(1, Map.of(OUT_OF_BOUNDS_REASON, reason));
    }
}
