/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/// Metrics describing the requests sent by search nodes to indexing nodes to read a chunk of a virtual batched compound commit
/// (see [TransportGetVirtualBatchedCompoundCommitChunkAction]). Two views are provided, both tagged with the [#OUTCOME_ATTRIBUTE]:
///
/// - per attempt ([#REQUEST_TOTAL_METRIC], [#REQUEST_DURATION_HISTOGRAM_METRIC]): time between sending a request to the indexing node
///   and receiving its response (or failure), measured before the response is used. Each retry is measured separately;
/// - per action execution ([#REQUEST_WITH_RETRIES_TOTAL_METRIC], [#REQUEST_WITH_RETRIES_DURATION_HISTOGRAM_METRIC]): time between the
///   start of the action on the search node and the completion of its listener, including all the retries and backoff delays.
public record GetVirtualBatchedCompoundCommitChunkMetrics(
    LongCounter requestCounter,
    LongHistogram requestDurationHistogram,
    LongCounter requestWithRetriesCounter,
    LongHistogram requestWithRetriesDurationHistogram
) {

    public static final String REQUEST_TOTAL_METRIC = "es.stateless.search.vbcc_chunk.requests.total";
    public static final String REQUEST_DURATION_HISTOGRAM_METRIC = "es.stateless.search.vbcc_chunk.requests.duration.histogram";
    public static final String REQUEST_WITH_RETRIES_TOTAL_METRIC = "es.stateless.search.vbcc_chunk.requests_including_retries.total";
    public static final String REQUEST_WITH_RETRIES_DURATION_HISTOGRAM_METRIC =
        "es.stateless.search.vbcc_chunk.requests_including_retries.duration.histogram";

    /// Whether the request completed with a response or with a failure.
    public static final String OUTCOME_ATTRIBUTE = "es_outcome";
    public static final String OUTCOME_SUCCESS = "success";
    public static final String OUTCOME_FAILURE = "failure";

    public static final GetVirtualBatchedCompoundCommitChunkMetrics NOOP = new GetVirtualBatchedCompoundCommitChunkMetrics(
        MeterRegistry.NOOP.registerLongCounter(REQUEST_TOTAL_METRIC + ".noop", "noop", "count"),
        MeterRegistry.NOOP.registerLongHistogram(REQUEST_DURATION_HISTOGRAM_METRIC + ".noop", "noop", "ms"),
        MeterRegistry.NOOP.registerLongCounter(REQUEST_WITH_RETRIES_TOTAL_METRIC + ".noop", "noop", "count"),
        MeterRegistry.NOOP.registerLongHistogram(REQUEST_WITH_RETRIES_DURATION_HISTOGRAM_METRIC + ".noop", "noop", "ms")
    );

    public static GetVirtualBatchedCompoundCommitChunkMetrics register(MeterRegistry registry) {
        return new GetVirtualBatchedCompoundCommitChunkMetrics(
            registry.registerLongCounter(
                REQUEST_TOTAL_METRIC,
                "Number of get virtual batched compound commit chunk requests sent",
                "count"
            ),
            registry.registerLongHistogram(
                REQUEST_DURATION_HISTOGRAM_METRIC,
                "Time in milliseconds between sending a get virtual batched compound commit chunk request and receiving its response",
                "ms"
            ),
            registry.registerLongCounter(
                REQUEST_WITH_RETRIES_TOTAL_METRIC,
                "Number of get virtual batched compound commit chunk actions executed, regardless of the number of retries",
                "count"
            ),
            registry.registerLongHistogram(
                REQUEST_WITH_RETRIES_DURATION_HISTOGRAM_METRIC,
                "Time in milliseconds taken by a get virtual batched compound commit chunk action, including retries",
                "ms"
            )
        );
    }

    /// Records a completed request.
    ///
    /// @param includeRetries `false` to record a single attempt, i.e. one request sent to the indexing node; `true` to record a whole
    /// action execution, including the retries
    /// @param success whether the request completed with a response
    /// @param durationInNanos the duration of the request, converted to milliseconds before being recorded
    public void record(boolean includeRetries, boolean success, long durationInNanos) {
        final var attributes = Map.<String, Object>of(OUTCOME_ATTRIBUTE, success ? OUTCOME_SUCCESS : OUTCOME_FAILURE);
        final long durationInMillis = TimeUnit.NANOSECONDS.toMillis(durationInNanos);
        if (includeRetries) {
            requestWithRetriesCounter.incrementBy(1L, attributes);
            requestWithRetriesDurationHistogram.record(durationInMillis, attributes);
        } else {
            requestCounter.incrementBy(1L, attributes);
            requestDurationHistogram.record(durationInMillis, attributes);
        }
    }
}
