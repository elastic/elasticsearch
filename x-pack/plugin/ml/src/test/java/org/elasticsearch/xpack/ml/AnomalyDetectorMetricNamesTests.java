/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;

import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnomalyDetectorMetricNamesTests extends ESTestCase {

    // The x-pack ML tests cannot depend on modules/apm, so this pattern mirrors MetricValidator.METRIC_PATTERN (with its
    // METRIC_SUFFIXES). Keep it in sync with that class. MetricValidator.assertValidAttributeNames additionally accepts the
    // dot-separated "es.ml.is_master" attribute on es.ml.* metrics; the total gauges record only that attribute, which
    // MlConfigMetricsTests asserts.
    private static final Pattern METRIC_PATTERN = Pattern.compile(
        "es(\\.[a-z][a-z0-9_]{0,29}){1,8}\\.(total|current|ratio|status|usage|size|utilization|histogram|time|duration)"
    );
    private static final Pattern ATTRIBUTE_PATTERN = Pattern.compile("es(_[a-z][a-z0-9]{0,29}){2,8}");
    private static final String IS_MASTER_ATTRIBUTE = "es.ml.is_master";

    private static final String HARD_LIMIT_METRIC = "es.ml.anomaly_detectors.hard_limit.current";
    private static final String JOB_TOTAL_METRIC = "es.ml.anomaly_detectors.total.current";
    private static final String DATAFEED_TOTAL_METRIC = "es.ml.datafeeds.total.current";

    public void testRegisteredGaugeNamesShouldPassMetricValidatorPattern() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        MlMetricsTests.startedMlNodeMetrics(meterRegistry, mock(AutodetectProcessManager.class), DiscoveryNodeRole.ML_ROLE);
        new MlConfigMetrics(
            meterRegistry,
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(JobConfigProvider.class),
            mock(DatafeedConfigProvider.class),
            Settings.builder().put(DiscoveryNodeRole.MASTER_ROLE.roleName(), true).build(),
            NamedXContentRegistry.EMPTY
        );

        List<String> gauges = meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_ASYNC_GAUGE);

        for (String name : List.of(HARD_LIMIT_METRIC, JOB_TOTAL_METRIC, DATAFEED_TOTAL_METRIC)) {
            assertThat(gauges, hasItem(name));
            assertThat("[" + name + "] must match the MetricValidator metric pattern", METRIC_PATTERN.matcher(name).matches(), is(true));
        }
    }

    public void testHardLimitGaugeAttributeNamesShouldPassMetricValidatorRules() {
        AutodetectProcessManager autodetectProcessManager = mock(AutodetectProcessManager.class);
        when(autodetectProcessManager.countJobsAtHardLimit()).thenReturn(1L);
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        MlMetricsTests.startedMlNodeMetrics(meterRegistry, autodetectProcessManager, DiscoveryNodeRole.ML_ROLE);
        meterRegistry.getRecorder().collect();

        List<Measurement> measurements = meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, HARD_LIMIT_METRIC);

        assertThat(measurements.size(), is(1));
        for (String attribute : measurements.get(0).attributes().keySet()) {
            assertThat(
                "[" + attribute + "] must match the MetricValidator attribute rules",
                IS_MASTER_ATTRIBUTE.equals(attribute) || ATTRIBUTE_PATTERN.matcher(attribute).matches(),
                is(true)
            );
        }
    }
}
