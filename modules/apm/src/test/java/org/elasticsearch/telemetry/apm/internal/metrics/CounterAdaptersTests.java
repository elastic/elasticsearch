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
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CounterAdaptersTests extends ESTestCase {
    RecordingOtelMeter otelMeter;
    APMMeterRegistry registry;

    @Before
    public void init() {
        otelMeter = new RecordingOtelMeter();
        registry = new APMMeterRegistry(otelMeter);
    }

    public void testLongCounterRecordsInitValue() {
        LongCounter longCounter = registry.registerLongCounter("es.test.long.count", "desc", "unit");

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(longCounter);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), is(nullValue()));
        assertThat(metrics.get(0).getLong(), equalTo(0L));
    }

    public void testDoubleCounterRecordsInitValue() {
        DoubleCounter doubleCounter = registry.registerDoubleCounter("es.test.double.count", "desc", "unit");

        List<Measurement> metrics = otelMeter.getRecorder().getMeasurements(doubleCounter);
        assertThat(metrics, hasSize(1));
        assertThat(metrics.get(0).attributes(), is(nullValue()));
        assertThat(metrics.get(0).getDouble(), equalTo(0.0));
    }
}
