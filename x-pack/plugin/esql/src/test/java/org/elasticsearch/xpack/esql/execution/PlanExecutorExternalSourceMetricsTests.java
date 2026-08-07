/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Wiring tests for {@link PlanExecutor#recordExternalSourceQuery}, the coordinator-side classification that turns a
 * completed query (its external-source flag, duration, partial flag and terminal failure) into the per-query
 * external-source metrics. They drive the real classification against a registry-backed {@link ExternalSourceMetrics}
 * (no mocks) and assert the observable registry output — mirroring the other telemetry wiring tests
 * ({@code ExternalSourceMetricsTests}, {@code AsyncExternalSourceOperatorTelemetryTests}).
 */
public class PlanExecutorExternalSourceMetricsTests extends ESTestCase {

    private RecordingMeterRegistry registry;
    private ExternalSourceMetrics metrics;

    @Before
    public void initMetrics() throws Exception {
        registry = new RecordingMeterRegistry();
        metrics = new ExternalSourceMetrics(registry);
    }

    public void testSuccessRecordsQueryTotalAndDurationWithOutcome() {
        PlanExecutor.recordExternalSourceQuery(metrics, true, 340L, false, null);

        Measurement total = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_TOTAL);
        assertThat(total.getLong(), equalTo(1L));
        assertThat(total.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE), equalTo(ExternalSourceMetrics.OUTCOME_SUCCESS));

        Measurement duration = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.QUERY_DURATION);
        assertThat(duration.getLong(), equalTo(340L));
        // The duration histogram carries the same outcome dimension so latency can be split by outcome.
        assertThat(duration.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE), equalTo(ExternalSourceMetrics.OUTCOME_SUCCESS));

        // A plain success touches neither the cancelled counter nor the breaker counter.
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_CANCELLED_TOTAL), hasSize(0));
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.BREAKER_TRIPPED_TOTAL), hasSize(0));
    }

    public void testPartialSuccessBumpsPartialCounter() {
        // A successful external-source query that returned partial results: outcome stays success, and the
        // partial flag drives queries.partial.total. This closes the coordinator-wiring gap for the partial
        // counter (the other outcomes are covered above; the holder-level partial path is covered by
        // ExternalSourceMetricsTests#testRecordQueryPartialAlsoBumpsPartialCounter).
        PlanExecutor.recordExternalSourceQuery(metrics, true, 77L, true, null);

        Measurement total = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_TOTAL);
        assertThat(total.getLong(), equalTo(1L));
        assertThat(total.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE), equalTo(ExternalSourceMetrics.OUTCOME_SUCCESS));

        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_PARTIAL_TOTAL).getLong(), equalTo(1L));
        // Partial is orthogonal to cancellation and breaker trips.
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_CANCELLED_TOTAL), hasSize(0));
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.BREAKER_TRIPPED_TOTAL), hasSize(0));
    }

    public void testCancellationClassifiesAsCancelled() {
        // A TaskCancelledException anywhere in the cause chain classifies the query as cancelled.
        Throwable failure = new RuntimeException("wrapped", new TaskCancelledException("task cancelled"));
        PlanExecutor.recordExternalSourceQuery(metrics, true, 12L, false, failure);

        Measurement total = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_TOTAL);
        assertThat(total.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE), equalTo(ExternalSourceMetrics.OUTCOME_CANCELLED));
        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_CANCELLED_TOTAL).getLong(), equalTo(1L));
        // Cancellation is not a breaker trip.
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.BREAKER_TRIPPED_TOTAL), hasSize(0));
    }

    public void testCircuitBreakerFailureBumpsBreakerTripped() {
        // A failure that unwraps to a CircuitBreakingException classifies as failure AND bumps breaker.tripped.
        Throwable failure = new ElasticsearchException(
            "wrapped",
            new CircuitBreakingException("es_datasource breaker tripped", CircuitBreaker.Durability.TRANSIENT)
        );
        PlanExecutor.recordExternalSourceQuery(metrics, true, 55L, false, failure);

        Measurement total = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_TOTAL);
        assertThat(total.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE), equalTo(ExternalSourceMetrics.OUTCOME_FAILURE));
        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.BREAKER_TRIPPED_TOTAL).getLong(), equalTo(1L));
        // A non-cancellation failure does not touch the cancelled counter.
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_CANCELLED_TOTAL), hasSize(0));
    }

    public void testNonExternalSourceQueryRecordsNothing() {
        // Not an external-source query: the whole external-source metric family stays untouched, whatever the outcome.
        PlanExecutor.recordExternalSourceQuery(metrics, false, 999L, false, null);
        PlanExecutor.recordExternalSourceQuery(
            metrics,
            false,
            999L,
            false,
            new CircuitBreakingException("ignored", CircuitBreaker.Durability.TRANSIENT)
        );

        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_TOTAL), hasSize(0));
        assertThat(measurements(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.QUERY_DURATION), hasSize(0));
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_CANCELLED_TOTAL), hasSize(0));
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.BREAKER_TRIPPED_TOTAL), hasSize(0));
    }

    private List<Measurement> measurements(InstrumentType type, String name) {
        return registry.getRecorder().getMeasurements(type, name);
    }

    private Measurement single(InstrumentType type, String name) {
        List<Measurement> found = measurements(type, name);
        assertThat("expected exactly one measurement for [" + name + "]", found, hasSize(1));
        return found.get(0);
    }
}
