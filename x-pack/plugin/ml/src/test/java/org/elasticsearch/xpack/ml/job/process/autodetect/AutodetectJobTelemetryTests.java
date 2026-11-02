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
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectJobTelemetry.OpenPhase;

import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class AutodetectJobTelemetryTests extends ESTestCase {

    // The x-pack ML tests cannot depend on modules/apm, so these patterns mirror MetricValidator.METRIC_PATTERN (with its
    // METRIC_SUFFIXES) and MetricValidator.Attributes.ATTRIBUTE_PATTERN. Keep them in sync with that class.
    private static final Pattern METRIC_PATTERN = Pattern.compile(
        "es(\\.[a-z][a-z0-9_]{0,29}){1,8}\\.(total|current|ratio|status|usage|size|utilization|histogram|time|duration)"
    );
    private static final Pattern ATTRIBUTE_PATTERN = Pattern.compile("es(_[a-z][a-z0-9]{0,29}){2,8}");

    private static final String TIME_TO_OPENED_METRIC = "es.ml.anomaly_detectors.time_to_opened.duration";
    private static final String RESULTS_LAG_METRIC = "es.ml.anomaly_detectors.results_lag.ratio";

    public void testRegisteredMetricNamesShouldPassMetricValidatorPattern() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        new AutodetectJobTelemetry(meterRegistry);

        assertThat(meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_HISTOGRAM), hasItem(TIME_TO_OPENED_METRIC));
        assertThat(meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.DOUBLE_HISTOGRAM), hasItem(RESULTS_LAG_METRIC));
        for (String name : List.of(TIME_TO_OPENED_METRIC, RESULTS_LAG_METRIC)) {
            assertThat("[" + name + "] must match the MetricValidator metric pattern", METRIC_PATTERN.matcher(name).matches(), is(true));
        }
    }

    public void testOpenPhaseAttributeNameShouldPassMetricValidatorPattern() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        AutodetectJobTelemetry telemetry = new AutodetectJobTelemetry(meterRegistry);

        for (OpenPhase phase : OpenPhase.values()) {
            telemetry.recordOpenPhaseDuration(phase, 1L);
        }

        List<Measurement> measurements = meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, TIME_TO_OPENED_METRIC);
        assertThat(measurements.size(), is(OpenPhase.values().length));
        for (Measurement measurement : measurements) {
            assertThat(measurement.attributes().size(), is(1));
            for (String attribute : measurement.attributes().keySet()) {
                assertThat(
                    "[" + attribute + "] must match the MetricValidator attribute pattern",
                    ATTRIBUTE_PATTERN.matcher(attribute).matches(),
                    is(true)
                );
            }
        }
    }

    public void testOpenPhaseValuesShouldBeLimitedToProcessStartAndStateWrite() {
        assertThat(
            List.of(OpenPhase.values()).stream().map(OpenPhase::attributeValue).toList(),
            is(List.of("process_start", "state_write"))
        );
    }

    public void testResultsLagRatioShouldBeAttributeFree() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        new AutodetectJobTelemetry(meterRegistry).recordResultsLagRatio(0.5);

        List<Measurement> measurements = meterRegistry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, RESULTS_LAG_METRIC);
        assertThat(measurements.size(), is(1));
        assertThat(measurements.get(0).attributes().isEmpty(), is(true));
    }
}
