/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class ForecastTelemetryTests extends ESTestCase {

    // The x-pack ML tests cannot depend on modules/apm, so this pattern mirrors MetricValidator.METRIC_PATTERN (with its
    // METRIC_SUFFIXES). Keep it in sync with that class.
    private static final Pattern METRIC_PATTERN = Pattern.compile(
        "es(\\.[a-z][a-z0-9_]{0,29}){1,8}\\.(total|current|ratio|status|usage|size|utilization|histogram|time|duration)"
    );

    private static final String REQUESTED_METRIC = "es.ml.forecasts.requested.total";
    private static final String FINISHED_METRIC = "es.ml.forecasts.finished.total";
    private static final String FAILED_METRIC = "es.ml.forecasts.failed.total";

    public void testRegisteredMetricNamesShouldPassMetricValidatorPattern() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        new ForecastTelemetry(meterRegistry);

        List<String> counters = meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_COUNTER);

        assertThat(counters, containsInAnyOrder(REQUESTED_METRIC, FINISHED_METRIC, FAILED_METRIC));
        for (String name : counters) {
            assertThat("[" + name + "] must match the MetricValidator metric pattern", METRIC_PATTERN.matcher(name).matches(), is(true));
        }
    }

    public void testRecordedCountersShouldIncrementByOneWithoutAttributes() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        ForecastTelemetry telemetry = new ForecastTelemetry(meterRegistry);

        telemetry.recordRequested();
        telemetry.recordFinished();
        telemetry.recordFailed();

        for (String name : List.of(REQUESTED_METRIC, FINISHED_METRIC, FAILED_METRIC)) {
            List<Measurement> measurements = meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, name);
            assertThat(measurements.size(), is(1));
            assertThat(measurements.get(0).getLong(), is(1L));
            // Job and forecast ids must never become attributes.
            assertThat(measurements.get(0).attributes().keySet(), is(empty()));
        }
    }

}
