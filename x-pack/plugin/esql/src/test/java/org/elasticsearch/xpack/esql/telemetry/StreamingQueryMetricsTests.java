/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class StreamingQueryMetricsTests extends ESTestCase {

    private RecordingMeterRegistry registry;
    private StreamingQueryMetrics metrics;

    @Before
    public void initMetrics() {
        registry = new RecordingMeterRegistry();
        metrics = new StreamingQueryMetrics(registry);
    }

    public void testClassifyOutcome() {
        assertThat(StreamingQueryMetrics.classifyOutcome(null), equalTo(StreamingQueryMetrics.OUTCOME_SUCCESS));
        assertThat(StreamingQueryMetrics.classifyOutcome(new RuntimeException("boom")), equalTo(StreamingQueryMetrics.OUTCOME_FAILURE));
        assertThat(
            StreamingQueryMetrics.classifyOutcome(new TaskCancelledException("cancelled")),
            equalTo(StreamingQueryMetrics.OUTCOME_CANCELLED)
        );
        assertThat(
            StreamingQueryMetrics.classifyOutcome(new RuntimeException("wrapper", new TaskCancelledException("cancelled"))),
            equalTo(StreamingQueryMetrics.OUTCOME_CANCELLED)
        );
    }

    public void testRecordSuccess() {
        metrics.record(null, false, true);

        assertThat(outcomeOfSingleTotal(), equalTo(StreamingQueryMetrics.OUTCOME_SUCCESS));
        assertNoMeasurements(StreamingQueryMetrics.QUERIES_PARTIAL_TOTAL);
        assertNoMeasurements(StreamingQueryMetrics.QUERIES_CANCELLED_TOTAL);
        assertNoMeasurements(StreamingQueryMetrics.QUERIES_FAILED_AFTER_HEADER_TOTAL);
    }

    public void testRecordPartialSuccess() {
        metrics.record(null, true, true);

        assertThat(outcomeOfSingleTotal(), equalTo(StreamingQueryMetrics.OUTCOME_SUCCESS));
        assertThat(single(StreamingQueryMetrics.QUERIES_PARTIAL_TOTAL).getLong(), equalTo(1L));
        assertNoMeasurements(StreamingQueryMetrics.QUERIES_FAILED_AFTER_HEADER_TOTAL);
    }

    public void testRecordFailureBeforeHeaderIsNotAFooterError() {
        metrics.record(new RuntimeException("boom"), false, false);

        assertThat(outcomeOfSingleTotal(), equalTo(StreamingQueryMetrics.OUTCOME_FAILURE));
        assertNoMeasurements(StreamingQueryMetrics.QUERIES_FAILED_AFTER_HEADER_TOTAL);
        assertNoMeasurements(StreamingQueryMetrics.QUERIES_CANCELLED_TOTAL);
    }

    public void testRecordFailureAfterHeaderIsAFooterError() {
        metrics.record(new RuntimeException("boom"), false, true);

        assertThat(outcomeOfSingleTotal(), equalTo(StreamingQueryMetrics.OUTCOME_FAILURE));
        assertThat(single(StreamingQueryMetrics.QUERIES_FAILED_AFTER_HEADER_TOTAL).getLong(), equalTo(1L));
        assertNoMeasurements(StreamingQueryMetrics.QUERIES_CANCELLED_TOTAL);
    }

    public void testRecordCancellationIsNotAFooterErrorWhetherOrNotHeaderFlushed() {
        boolean headerFlushed = randomBoolean();
        metrics.record(new TaskCancelledException("client disconnected"), false, headerFlushed);

        assertThat(outcomeOfSingleTotal(), equalTo(StreamingQueryMetrics.OUTCOME_CANCELLED));
        assertThat(single(StreamingQueryMetrics.QUERIES_CANCELLED_TOTAL).getLong(), equalTo(1L));
        assertNoMeasurements(StreamingQueryMetrics.QUERIES_FAILED_AFTER_HEADER_TOTAL);
    }

    public void testPopulateReportsUsageCounters() {
        metrics.record(null, false, true);
        metrics.record(null, true, true);
        metrics.record(new RuntimeException("before header"), false, false);
        metrics.record(new RuntimeException("after header"), false, true);
        metrics.record(new TaskCancelledException("cancelled"), false, true);

        Counters counters = new Counters();
        metrics.populate(counters);

        assertThat(counters.get("streaming.queries.by_outcome.success"), equalTo(2L));
        assertThat(counters.get("streaming.queries.by_outcome.failure"), equalTo(2L));
        assertThat(counters.get("streaming.queries.by_outcome.cancelled"), equalTo(1L));
        assertThat(counters.get("streaming.queries.partial.total"), equalTo(1L));
        assertThat(counters.get("streaming.queries.failed_after_header.total"), equalTo(1L));
    }

    public void testPopulateOnFreshMetricsReportsZeros() {
        Counters counters = new Counters();
        metrics.populate(counters);

        assertThat(counters.get("streaming.queries.by_outcome.success"), equalTo(0L));
        assertThat(counters.get("streaming.queries.by_outcome.failure"), equalTo(0L));
        assertThat(counters.get("streaming.queries.by_outcome.cancelled"), equalTo(0L));
        assertThat(counters.get("streaming.queries.partial.total"), equalTo(0L));
        assertThat(counters.get("streaming.queries.failed_after_header.total"), equalTo(0L));
    }

    private String outcomeOfSingleTotal() {
        return (String) single(StreamingQueryMetrics.QUERIES_TOTAL).attributes().get(StreamingQueryMetrics.OUTCOME_ATTRIBUTE);
    }

    private void assertNoMeasurements(String name) {
        assertThat(measurements(name), hasSize(0));
    }

    private List<Measurement> measurements(String name) {
        return registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, name);
    }

    private Measurement single(String name) {
        List<Measurement> found = measurements(name);
        assertThat("expected exactly one measurement for [" + name + "]", found, hasSize(1));
        return found.get(0);
    }
}
