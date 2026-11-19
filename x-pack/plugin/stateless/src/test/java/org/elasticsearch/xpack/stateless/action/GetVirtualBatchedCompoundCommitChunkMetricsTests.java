/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.OUTCOME_ATTRIBUTE;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.OUTCOME_FAILURE;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.OUTCOME_SUCCESS;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.REQUEST_DURATION_HISTOGRAM_METRIC;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.REQUEST_TOTAL_METRIC;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.REQUEST_WITH_RETRIES_DURATION_HISTOGRAM_METRIC;
import static org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkMetrics.REQUEST_WITH_RETRIES_TOTAL_METRIC;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class GetVirtualBatchedCompoundCommitChunkMetricsTests extends ESTestCase {

    public void testMetricNames() {
        assertThat(REQUEST_TOTAL_METRIC, equalTo("es.stateless.search.vbcc_chunk.requests.total"));
        assertThat(REQUEST_DURATION_HISTOGRAM_METRIC, equalTo("es.stateless.search.vbcc_chunk.requests.duration.histogram"));
        assertThat(REQUEST_WITH_RETRIES_TOTAL_METRIC, equalTo("es.stateless.search.vbcc_chunk.requests_including_retries.total"));
        assertThat(
            REQUEST_WITH_RETRIES_DURATION_HISTOGRAM_METRIC,
            equalTo("es.stateless.search.vbcc_chunk.requests_including_retries.duration.histogram")
        );
    }

    public void testRecordSingleRequestOnlyUpdatesRequestMetrics() {
        final var registry = new RecordingMeterRegistry();
        final var metrics = GetVirtualBatchedCompoundCommitChunkMetrics.register(registry);

        final boolean success = randomBoolean();
        final long millis = randomLongBetween(0, 10_000);
        metrics.record(false, success, millis);

        final var expectedAttributes = Map.<String, Object>of(OUTCOME_ATTRIBUTE, success ? OUTCOME_SUCCESS : OUTCOME_FAILURE);

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

        final boolean success = randomBoolean();
        final long millis = randomLongBetween(0, 10_000);
        metrics.record(true, success, millis);

        final var expectedAttributes = Map.<String, Object>of(OUTCOME_ATTRIBUTE, success ? OUTCOME_SUCCESS : OUTCOME_FAILURE);

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
