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
import org.elasticsearch.telemetry.metric.LongAsyncUpDownCounter;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LongAsyncUpDownCounterAdapterTests extends ESTestCase {
    RecordingOtelMeter otelMeter;
    APMMeterRegistry registry;

    @Before
    public void init() {
        otelMeter = new RecordingOtelMeter();
        registry = new APMMeterRegistry(otelMeter);
    }

    public void testLongAsyncUpDownCounter() {
        AtomicReference<Tuple<Long, Map<String, Object>>> attrs = new AtomicReference<>();
        LongAsyncUpDownCounter counter = registry.registerLongAsyncUpDownCounter("es.test.name.total", "desc", "unit", measurement -> {
            var observed = attrs.get();
            measurement.record(observed.v1(), observed.v2());
        });

        attrs.set(new Tuple<>(1L, Map.of("es_test_attribute", 1L)));

        otelMeter.collectMetrics();

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("es_test_attribute", 1L)));
        assertThat(metrics.get(0).getLong(), equalTo(1L));

        attrs.set(new Tuple<>(2L, Map.of("es_test_attribute", 5L)));

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("es_test_attribute", 5L)));
        assertThat(metrics.get(0).getLong(), equalTo(2L));

        counter.close();

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat(metrics, hasSize(0));
    }

    public void testNegativeValuesAreRecorded() {
        var counter = registry.registerLongAsyncUpDownCounter("es.test.name.total", "desc", "unit", () -> -3L);

        otelMeter.collectMetrics();

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).getLong(), equalTo(-3L));
    }

    public void testZeroValuesAreRecorded() {
        var counter = registry.registerLongAsyncUpDownCounter("es.test.name.total", "desc", "unit", () -> 0L);

        otelMeter.collectMetrics();

        assertThat(otelMeter.getRecorder().getMeasurements(counter), hasSize(1));
    }

    public void testWithInvalidAttribute() {
        registry.registerLongAsyncUpDownCounter(
            "es.test.name.total",
            "desc",
            "unit",
            measurement -> measurement.record(1L, Map.of("index", "index1"))
        );

        AssertionError error = assertThrows(AssertionError.class, otelMeter::collectMetrics);
        assertThat(error.getMessage(), containsString("Attribute [index] of [es.test.name.total] is forbidden"));
    }

    public void testIsRemovedFromTheRegistryAfterClosing() {
        var counter = registry.registerLongAsyncUpDownCounter("es.test.name.total", "desc", "thingies", () -> 2L);
        assertSame(counter, registry.getLongAsyncUpDownCounter(counter.getName()));

        otelMeter.collectMetrics();
        var metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).getLong(), equalTo(2L));

        counter.close();

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();
        metrics = otelMeter.getRecorder().getMeasurements(counter);
        assertThat("OTel SDK does not record anything anymore after the instrument is closed", metrics, hasSize(0));

        assertNull("Instrument is removed from the registry after closing", registry.getLongAsyncUpDownCounter(counter.getName()));
    }
}
