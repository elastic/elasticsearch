/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Node-level holder for ES|QL streaming-query operational metrics, published through the
 * node {@link MeterRegistry} (APM/OTLP) for dashboards and alerts, and also accumulated in
 * plain {@link Counters} for the {@code _xpack/usage} endpoint.
 *
 * <p>Mirrors the {@link org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics} idiom:
 * instruments are registered once in the constructor and recorded at the event. All recording
 * is best-effort — instrumentation failures are caught and logged at {@code TRACE} so they can
 * never affect query outcomes.
 *
 * <p>The footer-error-rate metric ({@link #QUERIES_FAILED_AFTER_HEADER_TOTAL}) is the primary new
 * signal this class adds: mid-stream failures return HTTP 200, so they are invisible to status-code
 * monitoring. A dedicated counter makes them alertable without parsing response bodies.
 */
public final class StreamingQueryMetrics {

    /**
     * One completed streaming query at the coordinator, dimensioned by {@link #OUTCOME_ATTRIBUTE}.
     * Incremented on every streaming query regardless of outcome.
     */
    public static final String QUERIES_TOTAL = "es.esql.streaming.queries.total";

    /** Streaming queries that returned partial results ({@code is_partial=true}). */
    public static final String QUERIES_PARTIAL_TOTAL = "es.esql.streaming.queries.partial.total";

    /** Streaming queries that ended in cancellation (client disconnect or task cancel). */
    public static final String QUERIES_CANCELLED_TOTAL = "es.esql.streaming.queries.cancelled.total";

    /**
     * Streaming queries that failed <em>after the HTTP header (column list) was already sent</em>.
     * These mid-stream failures return HTTP 200, so they are invisible to status-code monitoring.
     * This counter is the primary signal for footer-error-rate alerting. Cancellations (for example a
     * client disconnect) are excluded and counted under {@link #QUERIES_CANCELLED_TOTAL} instead.
     */
    public static final String QUERIES_FAILED_AFTER_HEADER_TOTAL = "es.esql.streaming.queries.failed_after_header.total";

    /**
     * Query-outcome dimension on {@link #QUERIES_TOTAL}, a closed low-cardinality set:
     * {@link #OUTCOME_SUCCESS}, {@link #OUTCOME_FAILURE}, {@link #OUTCOME_CANCELLED}.
     */
    public static final String OUTCOME_ATTRIBUTE = "es_streaming_outcome";

    /** Successful query outcome. */
    public static final String OUTCOME_SUCCESS = "success";

    /** Failed query outcome (non-cancellation error). */
    public static final String OUTCOME_FAILURE = "failure";

    /** Cancelled query outcome. */
    public static final String OUTCOME_CANCELLED = "cancelled";

    /**
     * No-op holder backed by {@link MeterRegistry#NOOP}, used where no node registry is available
     * (tests that do not exercise metrics). Call sites never branch on null.
     */
    public static final StreamingQueryMetrics NOOP = new StreamingQueryMetrics(MeterRegistry.NOOP);

    private static final Logger logger = LogManager.getLogger(StreamingQueryMetrics.class);

    private final LongCounter queriesTotal;
    private final LongCounter queriesPartialTotal;
    private final LongCounter queriesCancelledTotal;
    private final LongCounter queriesFailedAfterHeaderTotal;

    // Usage-stats accumulators, populated into Counters for _xpack/usage.
    private final LongAdder usageSuccess = new LongAdder();
    private final LongAdder usageFailure = new LongAdder();
    private final LongAdder usageCancelled = new LongAdder();
    private final LongAdder usagePartial = new LongAdder();
    private final LongAdder usageFailedAfterHeader = new LongAdder();

    public StreamingQueryMetrics(MeterRegistry meterRegistry) {
        queriesTotal = meterRegistry.registerLongCounter(QUERIES_TOTAL, "ES|QL streaming queries, dimensioned by outcome", "unit");
        queriesPartialTotal = meterRegistry.registerLongCounter(
            QUERIES_PARTIAL_TOTAL,
            "ES|QL streaming queries that returned partial results",
            "unit"
        );
        queriesCancelledTotal = meterRegistry.registerLongCounter(
            QUERIES_CANCELLED_TOTAL,
            "ES|QL streaming queries that ended in cancellation",
            "unit"
        );
        queriesFailedAfterHeaderTotal = meterRegistry.registerLongCounter(
            QUERIES_FAILED_AFTER_HEADER_TOTAL,
            "ES|QL streaming queries that failed mid-stream after the HTTP header was flushed. "
                + "These return HTTP 200 and are invisible to status-code monitoring.",
            "unit"
        );
    }

    /**
     * Records one completed streaming query. Best-effort: instrumentation failures are caught
     * and logged at {@code TRACE} so they can never affect query outcomes.
     *
     * @param failure       the terminal exception, or {@code null} on success
     * @param partial       whether the response carried {@code is_partial=true}
     * @param headerFlushed whether the HTTP response header (column list) had already been sent
     *                      when this query completed or failed; {@code true} means a failure
     *                      here returned HTTP 200 and is counted in
     *                      {@link #QUERIES_FAILED_AFTER_HEADER_TOTAL}
     */
    public void record(Throwable failure, boolean partial, boolean headerFlushed) {
        try {
            String outcome = classifyOutcome(failure);
            queriesTotal.incrementBy(1, Map.of(OUTCOME_ATTRIBUTE, outcome));
            switch (outcome) {
                case OUTCOME_SUCCESS -> usageSuccess.increment();
                case OUTCOME_FAILURE -> usageFailure.increment();
                case OUTCOME_CANCELLED -> {
                    queriesCancelledTotal.incrementBy(1);
                    usageCancelled.increment();
                }
                default -> throw new AssertionError("unexpected streaming query outcome: " + outcome);
            }
            if (partial) {
                queriesPartialTotal.incrementBy(1);
                usagePartial.increment();
            }
            if (failure != null && headerFlushed && OUTCOME_CANCELLED.equals(outcome) == false) {
                queriesFailedAfterHeaderTotal.incrementBy(1);
                usageFailedAfterHeader.increment();
            }
        } catch (Exception e) {
            logger.trace("telemetry: streaming query record failed", e);
        }
    }

    /**
     * Classifies a query outcome from the terminal exception, using the same taxonomy as
     * {@link org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics}:
     * {@code null} → {@link #OUTCOME_SUCCESS}; a {@link TaskCancelledException} anywhere in
     * the cause chain → {@link #OUTCOME_CANCELLED}; anything else → {@link #OUTCOME_FAILURE}.
     *
     * <p>Package-private: used by {@link #record} and directly by the unit test.
     */
    static String classifyOutcome(Throwable failure) {
        if (failure == null) {
            return OUTCOME_SUCCESS;
        } else if (ExceptionsHelper.unwrap(failure, TaskCancelledException.class) != null) {
            return OUTCOME_CANCELLED;
        } else {
            return OUTCOME_FAILURE;
        }
    }

    /**
     * Populates the streaming subset of the ES|QL {@code _xpack/usage} {@link Counters}.
     * Called from {@code TransportEsqlStatsAction} alongside
     * {@link org.elasticsearch.xpack.esql.datasources.DataSourceCounters#populate}.
     */
    public void populate(Counters counters) {
        counters.inc("streaming.queries.by_outcome." + OUTCOME_SUCCESS, usageSuccess.longValue());
        counters.inc("streaming.queries.by_outcome." + OUTCOME_FAILURE, usageFailure.longValue());
        counters.inc("streaming.queries.by_outcome." + OUTCOME_CANCELLED, usageCancelled.longValue());
        counters.inc("streaming.queries.partial.total", usagePartial.longValue());
        counters.inc("streaming.queries.failed_after_header.total", usageFailedAfterHeader.longValue());
    }
}
