/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class GaugeAdapterTests extends ESTestCase {
    RecordingMeterProvider meter;

    @Before
    public void init() {
        meter = new RecordingMeterProvider();
    }

    // testing that a value reported is then used in a callback
    @SuppressWarnings("unchecked")
    public void testLongGaugeRecord() {
        LongGaugeAdapter gauge = new LongGaugeAdapter(meter, "name", "desc", "unit");

        // recording values;
        Map<String, Object> m1 = Map.of("k", 1L);
        Map<String, Object> m3 = Map.of("k", 2L);
        Map<String, Object> m4 = Map.of("k", 1L, "j", 3L);
        gauge.record(1L, m1);
        gauge.record(2L, m1);
        gauge.record(3L, m3);
        gauge.record(4L, m4);

        meter.collectMetrics();

        List<TestMetric> metrics = meter.getRecorder().get(gauge);

        assertThat(metrics, hasSize(3));

        assertThat(metrics, containsInAnyOrder(new TestMetric(2L, m1), new TestMetric(3L, m3), new TestMetric(4L, m4)));

        meter.clearCalls();

        meter.collectMetrics();
        metrics = meter.getRecorder().get(gauge);
        assertThat(metrics, empty());

        gauge.record(1L, m1);
        gauge.record(3L, m3);

        meter.collectMetrics();
        metrics = meter.getRecorder().get(gauge);
        assertThat(metrics, hasSize(2));

        assertThat(metrics, containsInAnyOrder(new TestMetric(1L, m1), new TestMetric(3L, m3)));
    }

    @SuppressWarnings("unchecked")
    public void testDoubleGaugeRecord() {
        DoubleGaugeAdapter gauge = new DoubleGaugeAdapter(meter, "name", "desc", "unit");

        // recording values;
        Map<String, Object> m1 = Map.of("k", 1L);
        Map<String, Object> m3 = Map.of("k", 2L);
        Map<String, Object> m4 = Map.of("k", 1L, "j", 3L);
        gauge.record(1.0, m1);
        gauge.record(2.0, m1);
        gauge.record(3.0, m3);
        gauge.record(4.0, m4);

        meter.collectMetrics();

        List<TestMetric> metrics = meter.getRecorder().get(gauge);

        assertThat(metrics, hasSize(3));

        assertThat(metrics, containsInAnyOrder(new TestMetric(2.0, m1), new TestMetric(3.0, m3), new TestMetric(4.0, m4)));

        meter.clearCalls();
        meter.collectMetrics();
        metrics = meter.getRecorder().get(gauge);
        assertThat(metrics, empty());

        gauge.record(1.0, m1);
        gauge.record(3.0, m3);

        meter.collectMetrics();
        metrics = meter.getRecorder().get(gauge);
        assertThat(metrics, hasSize(2));

        assertThat(metrics, containsInAnyOrder(new TestMetric(1.0, m1), new TestMetric(3.0, m3)));
    }

    public void testDifferentLongGaugesSameValues() {
        LongGaugeAdapter gauge1 = new LongGaugeAdapter(meter, "name1", "desc", "unit");
        LongGaugeAdapter gauge2 = new LongGaugeAdapter(meter, "name2", "desc", "unit");
        Map<String, Object> map = Map.of("k", 1L);
        gauge1.record(1L, map);
        gauge2.record(2L, map);

        meter.collectMetrics();
        List<TestMetric> metrics = meter.getRecorder().get(gauge1);
        assertThat(metrics, hasSize(1));
        assertThat(metrics, contains(new TestMetric(1L, map)));

        metrics = meter.getRecorder().get(gauge2);
        assertThat(metrics, hasSize(1));
        assertThat(metrics, contains(new TestMetric(2L, map)));
    }

    public void testDifferentDoubleGaugesSameValues() {
        DoubleGaugeAdapter gauge1 = new DoubleGaugeAdapter(meter, "name1", "desc", "unit");
        DoubleGaugeAdapter gauge2 = new DoubleGaugeAdapter(meter, "name2", "desc", "unit");
        Map<String, Object> map = Map.of("k", 1L);
        gauge1.record(1.0, map);
        gauge2.record(2.0, map);

        meter.collectMetrics();
        List<TestMetric> metrics = meter.getRecorder().get(gauge1);
        assertThat(metrics, hasSize(1));
        assertThat(metrics, contains(new TestMetric(1.0, map)));

        metrics = meter.getRecorder().get(gauge2);
        assertThat(metrics, hasSize(1));
        assertThat(metrics, contains(new TestMetric(2.0, map)));
    }
}
