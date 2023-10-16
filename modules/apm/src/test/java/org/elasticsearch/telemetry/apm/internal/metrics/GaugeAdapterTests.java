/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.telemetry.apm.TestAPMMeterService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class GaugeAdapterTests extends ESTestCase {
    TestAPMMeterService meterService;
    Meter meter;

    @Before
    public void init() {
        meterService = new TestAPMMeterService();
        meter = meterService.getMeter();
    }

    // testing that a value reported is then used in a callback
    @SuppressWarnings("unchecked")
    public void testLongGaugeRecord() {
        LongGaugeAdapter longGaugeAdapter = new LongGaugeAdapter(meter, "name", "desc", "unit");

        // recording a value
        Map<String, Object> attributes = Map.of("k", 1L);
        longGaugeAdapter.record(1L, attributes);

        meterService.collectMetrics();

        List<TestAPMMeterService.Metric> metrics = meterService.getMetrics(longGaugeAdapter, "name");
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).getAttributes(), equalTo(attributes));
        assertThat(metrics.get(0).getNumber(), equalTo(1L));
    }

    // testing that a value reported is then used in a callback
    @SuppressWarnings("unchecked")
    public void testDoubleGaugeRecord() {
        DoubleGaugeAdapter doubleGaugeAdapter = new DoubleGaugeAdapter(meter, "name", "desc", "unit");
        Map<String, Object> attributes = Map.of("k", 1L);
        doubleGaugeAdapter.record(1.0, attributes);
        meterService.collectMetrics();

        List<TestAPMMeterService.Metric> metrics = meterService.getMetrics(doubleGaugeAdapter, "name");
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).getAttributes(), equalTo(attributes));
        assertThat(metrics.get(0).getNumber(), equalTo(1.0));
    }
}
