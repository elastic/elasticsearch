/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.apm.APMMeterRegistry;
import org.elasticsearch.telemetry.apm.RecordingOtelMeter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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
    public void testLongGaugeRecord() throws Exception {
        AtomicReference<LongWithAttributes> attrs = new AtomicReference<>();
        LongGauge gauge = registry.registerLongGauge("es.test.name.total", "desc", "unit", attrs::get);

        attrs.set(new LongWithAttributes(1L, Map.of("k", 1L)));

        otelMeter.collectMetrics();

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(gauge);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("k", 1L)));
        assertThat(metrics.get(0).getLong(), equalTo(1L));

        attrs.set(new LongWithAttributes(2L, Map.of("k", 5L)));

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(gauge);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("k", 5L)));
        assertThat(metrics.get(0).getLong(), equalTo(2L));

        gauge.close();

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(gauge);
        assertThat(metrics, hasSize(0));
    }

    // testing that a value reported is then used in a callback
    public void testDoubleGaugeRecord() throws Exception {
        AtomicReference<DoubleWithAttributes> attrs = new AtomicReference<>();
        DoubleGauge gauge = registry.registerDoubleGauge("es.test.name.total", "desc", "unit", attrs::get);

        attrs.set(new DoubleWithAttributes(1.0d, Map.of("k", 1L)));

        otelMeter.collectMetrics();

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(gauge);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("k", 1L)));
        assertThat(metrics.get(0).getDouble(), equalTo(1.0d));

        attrs.set(new DoubleWithAttributes(2.0d, Map.of("k", 5L)));

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(gauge);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("k", 5L)));
        assertThat(metrics.get(0).getDouble(), equalTo(2.0d));

        gauge.close();

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(gauge);
        assertThat(metrics, hasSize(0));
    }

    public void testNullGaugeRecord() throws Exception {
        DoubleGauge dgauge = registry.registerDoubleGauge(
            "es.test.name.total",
            "desc",
            "unit",
            new AtomicReference<DoubleWithAttributes>()::get
        );
        otelMeter.collectMetrics();
        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(dgauge);
        assertThat(metrics, hasSize(0));

        LongGauge lgauge = registry.registerLongGauge("es.test.name.total", "desc", "unit", new AtomicReference<LongWithAttributes>()::get);
        otelMeter.collectMetrics();
        metrics = otelMeter.getRecorder().getMeasurements(lgauge);
        assertThat(metrics, hasSize(0));
    }
}
