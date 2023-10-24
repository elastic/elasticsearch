/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.apm.APMMeterRegistry;
import org.elasticsearch.telemetry.apm.RecordingOtelMeter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class GaugeAdapterTests extends ESTestCase {
    RecordingOtelMeter otelMeter;
    APMMeterRegistry registry;

    @Before
    public void init() {
        otelMeter = new RecordingOtelMeter();
        registry = new APMMeterRegistry(otelMeter);
    }

    // testing that a value reported is then used in a callback
    @SuppressWarnings("unchecked")
    public void testLongGaugeRecord() {
        LongGauge longGauge = registry.registerLongGauge("name", "desc", "unit");

        // recording a value
        Map<String, Object> attributes = Map.of("k", 1L);
        longGauge.record(1L, attributes);

        otelMeter.collectMetrics();

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(longGauge);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(attributes));
        assertThat(metrics.get(0).getLong(), equalTo(1L));
    }

    // testing that a value reported is then used in a callback
    @SuppressWarnings("unchecked")
    public void testDoubleGaugeRecord() {
        DoubleGauge doubleGauge = registry.registerDoubleGauge("name", "desc", "unit");
        Map<String, Object> attributes = Map.of("k", 1L);
        doubleGauge.record(1.0, attributes);

        otelMeter.collectMetrics();

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(doubleGauge);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(attributes));
        assertThat(metrics.get(0).getDouble(), equalTo(1.0));
    }
}
