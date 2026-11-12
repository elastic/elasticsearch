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
import org.elasticsearch.telemetry.metric.DoubleAsyncUpDownCounter;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DoubleAsyncUpDownCounterAdapterTests extends ESTestCase {
    RecordingOtelMeter otelMeter;
    APMMeterRegistry registry;

    @Before
    public void init() {
        otelMeter = new RecordingOtelMeter();
        registry = new APMMeterRegistry(otelMeter);
    }

    public void testDoubleAsyncUpDownCounter() {
        AtomicReference<Tuple<Double, Map<String, Object>>> attrs = new AtomicReference<>();
        DoubleAsyncUpDownCounter counter = registry.registerDoubleAsyncUpDownCounter("es.test.name.total", "desc", "unit", measurement -> {
            var observed = attrs.get();
            measurement.record(observed.v1(), observed.v2());
        });

        attrs.set(new Tuple<>(1.0, Map.of("es_test_attribute", 1.0)));

        otelMeter.collectMetrics();

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("es_test_attribute", 1.0)));
        assertThat(metrics.get(0).getDouble(), equalTo(1.0));

        attrs.set(new Tuple<>(2.0, Map.of("es_test_attribute", 5.0)));

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("es_test_attribute", 5.0)));
        assertThat(metrics.get(0).getDouble(), equalTo(2.0));

        counter.close();

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat(metrics, hasSize(0));
    }

    public void testNegativeValuesAreRecorded() {
        var counter = registry.registerDoubleAsyncUpDownCounter("es.test.name.total", "desc", "unit", () -> -3.0);

        otelMeter.collectMetrics();

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).getDouble(), equalTo(-3.0));
    }

    public void testZeroValuesAreRecorded() {
        var counter = registry.registerDoubleAsyncUpDownCounter("es.test.name.total", "desc", "unit", () -> 0.0);

        otelMeter.collectMetrics();

        assertThat(otelMeter.getRecorder().getMeasurements(counter), hasSize(1));
    }

    public void testWithInvalidAttribute() {
        registry.registerDoubleAsyncUpDownCounter(
            "es.test.name.total",
            "desc",
            "unit",
            measurement -> measurement.record(1.0, Map.of("es_has_timestamp", "false"))
        );

        AssertionError error = assertThrows(AssertionError.class, otelMeter::collectMetrics);
        assertThat(error.getMessage(), containsString("Attribute [es_has_timestamp] of [es.test.name.total] is forbidden"));
    }

    public void testIsRemovedFromTheRegistryAfterClosing() {
        var counter = registry.registerDoubleAsyncUpDownCounter("es.test.name.total", "desc", "thingies", () -> 2.0);
        assertSame(counter, registry.getDoubleAsyncUpDownCounter(counter.getName()));

        otelMeter.collectMetrics();
        var metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).getDouble(), equalTo(2.0));

        counter.close();

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();
        metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat("OTel SDK does not record anything anymore after the instrument is closed", metrics, hasSize(0));

        assertNull("Instrument is removed from the registry after closing", registry.getDoubleAsyncUpDownCounter(counter.getName()));
    }
}
