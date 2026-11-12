/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.DatafeedSearchTelemetry;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.MlNotificationTelemetry;

import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class MlCounterMetricNamesTests extends ESTestCase {

    // The x-pack ML tests cannot depend on modules/apm, so these patterns mirror MetricValidator.METRIC_PATTERN (with its
    // METRIC_SUFFIXES) and MetricValidator.Attributes.ATTRIBUTE_PATTERN. Keep them in sync with that class.
    private static final Pattern METRIC_PATTERN = Pattern.compile(
        "es(\\.[a-z][a-z0-9_]{0,29}){1,8}\\.(total|current|ratio|status|usage|size|utilization|histogram|time|duration)"
    );
    private static final Pattern ATTRIBUTE_PATTERN = Pattern.compile("es(_[a-z][a-z0-9]{0,29}){2,8}");

    private static final List<String> EXPECTED_COUNTERS = List.of(
        "es.ml.datafeeds.records.processed.total",
        "es.ml.datafeeds.records.invalid_date.total",
        "es.ml.datafeeds.records.missing_field.total",
        "es.ml.maintenance.runs.total",
        "es.ml.notifications.total"
    );
    private static final String EXPECTED_GAUGE = "es.ml.maintenance.last_success_age.current";

    public void testRegisteredMetricNamesShouldPassMetricValidatorPattern() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        new DatafeedSearchTelemetry(meterRegistry);
        new MlNotificationTelemetry(meterRegistry);
        MlDailyMaintenanceService service = new MlDailyMaintenanceService(
            Settings.EMPTY,
            new DeterministicTaskQueue().getThreadPool(),
            mock(Client.class),
            mock(ClusterService.class),
            mock(AnomalyDetectionAuditor.class),
            mock(MlAssignmentNotifier.class),
            () -> TimeValue.timeValueHours(24),
            TestIndexNameExpressionResolver.newInstance(),
            true,
            true,
            true,
            true,
            meterRegistry
        );
        service.start();

        List<String> counters = meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_COUNTER);
        List<String> gauges = meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_ASYNC_GAUGE);

        assertThat(counters, hasItems(EXPECTED_COUNTERS.toArray(String[]::new)));
        assertThat(gauges, hasItems(EXPECTED_GAUGE));
        for (String name : EXPECTED_COUNTERS) {
            assertThat("[" + name + "] must match the MetricValidator metric pattern", METRIC_PATTERN.matcher(name).matches(), is(true));
        }
        assertThat(
            "[" + EXPECTED_GAUGE + "] must match the MetricValidator metric pattern",
            METRIC_PATTERN.matcher(EXPECTED_GAUGE).matches(),
            is(true)
        );
        service.stop();
    }

    public void testAttributeNamesShouldPassMetricValidatorPattern() {
        for (String attribute : List.of(MlDailyMaintenanceService.RUN_RESULT_ATTRIBUTE, MlNotificationTelemetry.LEVEL_ATTRIBUTE)) {
            assertThat(
                "[" + attribute + "] must match the MetricValidator attribute pattern",
                ATTRIBUTE_PATTERN.matcher(attribute).matches(),
                is(true)
            );
        }
    }
}
