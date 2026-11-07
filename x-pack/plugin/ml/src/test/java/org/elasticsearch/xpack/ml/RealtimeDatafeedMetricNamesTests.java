/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.DatafeedHorizonTelemetry;
import org.elasticsearch.xpack.ml.datafeed.DatafeedSearchTelemetry.ExtractorType;

import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class RealtimeDatafeedMetricNamesTests extends ESTestCase {

    // The x-pack ML tests cannot depend on modules/apm, so these patterns mirror MetricValidator.METRIC_PATTERN (with its
    // METRIC_SUFFIXES) and MetricValidator.Attributes.ATTRIBUTE_PATTERN. Keep them in sync with that class.
    private static final Pattern METRIC_PATTERN = Pattern.compile(
        "es(\\.[a-z][a-z0-9_]{0,29}){1,8}\\.(total|current|ratio|status|usage|size|utilization|histogram|time|duration)"
    );
    private static final Pattern ATTRIBUTE_PATTERN = Pattern.compile("es(_[a-z][a-z0-9]{0,29}){2,8}");

    private static final String HORIZON_LAG_METRIC = "es.ml.datafeeds.realtime.horizon_lag.ratio";
    private static final String REALTIME_EXPECTED_METRIC = "es.ml.datafeeds.realtime_expected.current";

    public void testRegisteredMetricNamesShouldPassMetricValidatorPattern() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        new DatafeedHorizonTelemetry(meterRegistry);
        MlMetricsTests.startedMasterMetrics(meterRegistry);

        List<String> histograms = meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.DOUBLE_HISTOGRAM);
        List<String> gauges = meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_ASYNC_GAUGE);

        assertThat(histograms, hasItem(HORIZON_LAG_METRIC));
        assertThat(gauges, hasItem(REALTIME_EXPECTED_METRIC));
        for (String name : List.of(HORIZON_LAG_METRIC, REALTIME_EXPECTED_METRIC)) {
            assertThat("[" + name + "] must match the MetricValidator metric pattern", METRIC_PATTERN.matcher(name).matches(), is(true));
        }
    }

    public void testHorizonLagAttributeNamesShouldPassMetricValidatorPattern() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        DatafeedHorizonTelemetry telemetry = new DatafeedHorizonTelemetry(meterRegistry);

        telemetry.recordHorizonLagRatio(ExtractorType.SCROLL, 1.0);

        List<Measurement> measurements = meterRegistry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, HORIZON_LAG_METRIC);
        assertThat(measurements.size(), is(1));
        for (String attribute : measurements.get(0).attributes().keySet()) {
            assertThat(
                "[" + attribute + "] must match the MetricValidator attribute pattern",
                ATTRIBUTE_PATTERN.matcher(attribute).matches(),
                is(true)
            );
        }
    }
}
