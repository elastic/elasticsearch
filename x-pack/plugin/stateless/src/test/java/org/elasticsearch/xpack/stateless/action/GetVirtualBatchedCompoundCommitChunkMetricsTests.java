/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.Scope;

import java.util.Map;

import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.OUTCOME_ATTRIBUTE;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.OUTCOME_FAILURE;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.OUTCOME_SUCCESS;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.OUTCOME_UPLOADED;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.REQUEST_DURATION_HISTOGRAM_METRIC;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.REQUEST_TOTAL_METRIC;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.REQUEST_WITH_RETRIES_DURATION_HISTOGRAM_METRIC;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.REQUEST_WITH_RETRIES_TOTAL_METRIC;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class GetVirtualBatchedCompoundCommitChunkMetricsTests extends ESTestCase {

    /// Failures to record, with the outcome expected for each of them. A `null` failure stands for a request completed with a response.
    private record Case(Exception failure, String expectedOutcome) {}

    private static Case randomCase() {
        return randomFrom(
            new Case(null, OUTCOME_SUCCESS),
            // ResourceNotFoundException is how the action signals that the VBCC has been uploaded: not an actual failure
            new Case(new ResourceNotFoundException("uploaded"), OUTCOME_UPLOADED),
            new Case(new RemoteTransportException("wrapped", new ResourceNotFoundException("uploaded")), OUTCOME_UPLOADED),
            new Case(new ConnectTransportException(null, "simulated"), OUTCOME_FAILURE),
            new Case(new IllegalStateException("unexpected"), OUTCOME_FAILURE)
        );
    }

    public void testRecordSingleRequestOnlyUpdatesRequestMetrics() {
        final var registry = new RecordingMeterRegistry();
        final var metrics = GetVirtualBatchedCompoundCommitChunkMetrics.register(registry);

        final var testCase = randomCase();
        final long millis = randomLongBetween(0, 10_000);
        metrics.record(Scope.SINGLE_REQUEST, testCase.failure(), millis);

        final var expectedAttributes = Map.<String, Object>of(OUTCOME_ATTRIBUTE, testCase.expectedOutcome());

        final var counts = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REQUEST_TOTAL_METRIC);
        assertThat(counts, hasSize(1));
        assertThat(counts.getFirst().getLong(), equalTo(1L));
        assertThat(counts.getFirst().attributes(), equalTo(expectedAttributes));

        final var durations = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REQUEST_DURATION_HISTOGRAM_METRIC);
        assertThat(durations, hasSize(1));
        assertThat(durations.getFirst().getLong(), equalTo(millis));
        assertThat(durations.getFirst().attributes(), equalTo(expectedAttributes));

        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REQUEST_WITH_RETRIES_TOTAL_METRIC), empty());
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REQUEST_WITH_RETRIES_DURATION_HISTOGRAM_METRIC),
            empty()
        );
    }

    public void testRecordRequestIncludingRetriesOnlyUpdatesRetriesMetrics() {
        final var registry = new RecordingMeterRegistry();
        final var metrics = GetVirtualBatchedCompoundCommitChunkMetrics.register(registry);

        final var testCase = randomCase();
        final long millis = randomLongBetween(0, 10_000);
        metrics.record(Scope.END_TO_END, testCase.failure(), millis);

        final var expectedAttributes = Map.<String, Object>of(OUTCOME_ATTRIBUTE, testCase.expectedOutcome());

        final var counts = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REQUEST_WITH_RETRIES_TOTAL_METRIC);
        assertThat(counts, hasSize(1));
        assertThat(counts.getFirst().getLong(), equalTo(1L));
        assertThat(counts.getFirst().attributes(), equalTo(expectedAttributes));

        final var durations = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, REQUEST_WITH_RETRIES_DURATION_HISTOGRAM_METRIC);
        assertThat(durations, hasSize(1));
        assertThat(durations.getFirst().getLong(), equalTo(millis));
        assertThat(durations.getFirst().attributes(), equalTo(expectedAttributes));

        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REQUEST_TOTAL_METRIC), empty());
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REQUEST_DURATION_HISTOGRAM_METRIC), empty());
    }
}
