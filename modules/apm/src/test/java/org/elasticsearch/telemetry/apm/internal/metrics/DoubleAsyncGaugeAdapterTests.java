/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.apm.RecordingOtelMeter;
import org.elasticsearch.telemetry.metric.DoubleAsyncGauge;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DoubleAsyncGaugeAdapterTests extends ESTestCase {
    RecordingOtelMeter otelMeter;
    APMMeterRegistry registry;

    @Before
    public void init() {
        otelMeter = new RecordingOtelMeter();
        registry = new APMMeterRegistry(otelMeter);
    }

    public void testDoubleGaugeRecord() {
        AtomicReference<Tuple<Double, Map<String, Object>>> attrs = new AtomicReference<>();
        DoubleAsyncGauge gauge = registry.registerDoubleAsyncGauge("es.test.name.total", "desc", "unit", measurement -> {
            var value = attrs.get();
            measurement.record(value.v1(), value.v2());
        });

        attrs.set(new Tuple<>(1.0d, Map.of("es_test_attribute", 1L)));

        otelMeter.collectMetrics();

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(gauge);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("es_test_attribute", 1L)));
        assertThat(metrics.get(0).getDouble(), equalTo(1.0d));

        attrs.set(new Tuple<>(2.0d, Map.of("es_test_attribute", 5L)));

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(gauge);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("es_test_attribute", 5L)));
        assertThat(metrics.get(0).getDouble(), equalTo(2.0d));

        gauge.close();

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(gauge);
        assertThat(metrics, hasSize(0));
    }

    public void testZeroValuedGaugeIsRecorded() {
        DoubleAsyncGauge doubleAsyncGauge = registry.registerDoubleAsyncGauge("es.test.double.current", "desc", "unit", () -> 0.0);

        otelMeter.collectMetrics();

        assertThat(otelMeter.getRecorder().getMeasurements(doubleAsyncGauge), hasSize(1));
    }

    public void testDoubleGaugeWithInvalidAttribute() {
        registry.registerDoubleAsyncGauge(
            "es.test.name.total",
            "desc",
            "unit",
            measurement -> measurement.record(1.0, Map.of("es_has_timestamp", "false"))
        );

        AssertionError error = assertThrows(AssertionError.class, otelMeter::collectMetrics);
        assertThat(error.getMessage(), containsString("Attribute [es_has_timestamp] of [es.test.name.total] is forbidden"));
    }

    public void testDoubleGaugeIsRemovedFromTheRegistryAfterClosing() {
        var gauge = registry.registerDoubleAsyncGauge("es.test.name.total", "desc", "thingies", () -> 42.0);

        otelMeter.collectMetrics();
        var metrics = otelMeter.getRecorder().getMeasurements(gauge);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).getDouble(), equalTo(42.0));

        gauge.close();

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();
        metrics = otelMeter.getRecorder().getMeasurements(gauge);
        assertThat("OTel SDK does not record anything anymore after the instrument is closed", metrics, hasSize(0));

        assertNull("Instrument is removed from the registry after closing", registry.getDoubleGauge(gauge.getName()));
    }
}
