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
import org.elasticsearch.telemetry.metric.DoubleAttributes;
import org.elasticsearch.telemetry.metric.DoubleGaugeObserver;
import org.elasticsearch.telemetry.metric.LongAttributes;
import org.elasticsearch.telemetry.metric.LongGaugeObserver;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class GaugeAdapterObserverTests extends ESTestCase {
    RecordingOtelMeter otelMeter;
    APMMeterRegistry registry;

    @Before
    public void init() {
        otelMeter = new RecordingOtelMeter();
        registry = new APMMeterRegistry(otelMeter);
    }

    // testing that a value reported is then used in a callback
    public void testLongGaugeRecord() {
        LongGaugeObserver longGaugeObserver = registry.registerLongGaugeObserver("name", "desc", "unit");

        AtomicReference<LongAttributes> attrs = new AtomicReference<>();
        longGaugeObserver.setObserver(attrs::get);

        attrs.set(new LongAttributes(1L, Map.of("k", 1L)));

        otelMeter.collectMetrics();

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(longGaugeObserver);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("k", 1L)));
        assertThat(metrics.get(0).getLong(), equalTo(1L));

        attrs.set(new LongAttributes(2L, Map.of("k", 5L)));

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(longGaugeObserver);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("k", 5L)));
        assertThat(metrics.get(0).getLong(), equalTo(2L));

        longGaugeObserver.setObserver(() -> new LongAttributes(100L, Map.of("k", 200)));

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(longGaugeObserver);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("k", 200L))); // int -> long for attributes
        assertThat(metrics.get(0).getLong(), equalTo(100L));
    }

    // testing that a value reported is then used in a callback
    public void testDoubleGaugeRecord() {
        DoubleGaugeObserver doubleGaugeObserver = registry.registerDoubleGaugeObserver("name", "desc", "unit");

        AtomicReference<DoubleAttributes> attrs = new AtomicReference<>();
        doubleGaugeObserver.setObserver(attrs::get);

        attrs.set(new DoubleAttributes(1.0d, Map.of("k", 1L)));

        otelMeter.collectMetrics();

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(doubleGaugeObserver);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("k", 1L)));
        assertThat(metrics.get(0).getDouble(), equalTo(1.0d));

        attrs.set(new DoubleAttributes(2.0d, Map.of("k", 5L)));

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(doubleGaugeObserver);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("k", 5L)));
        assertThat(metrics.get(0).getDouble(), equalTo(2.0d));

        doubleGaugeObserver.setObserver(() -> new DoubleAttributes(100.0d, Map.of("k", 200)));

        otelMeter.getRecorder().resetCalls();
        otelMeter.collectMetrics();

        metrics = otelMeter.getRecorder().getMeasurements(doubleGaugeObserver);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), equalTo(Map.of("k", 200L))); // int -> long for attributes
        assertThat(metrics.get(0).getDouble(), equalTo(100.0d));
    }
}
