/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.LongGaugeBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LongGaugeAdapterTests extends ESTestCase {
    Meter testMeter = Mockito.mock(Meter.class);
    LongGaugeBuilder longGaugeBuilder = Mockito.mock(LongGaugeBuilder.class);

    @Before
    public void init(){
        when(longGaugeBuilder.setDescription(Mockito.anyString())).thenReturn(longGaugeBuilder);
        when(longGaugeBuilder.setUnit(Mockito.anyString())).thenReturn(longGaugeBuilder);

        DoubleGaugeBuilder mockDoubleGaugeBuilder = Mockito.mock(DoubleGaugeBuilder.class);

        when(mockDoubleGaugeBuilder.ofLongs()).thenReturn(longGaugeBuilder);
        when(testMeter.gaugeBuilder(anyString())).thenReturn(mockDoubleGaugeBuilder);
    }

    public void testLongGaugeRecord() {
        LongGaugeAdapter longGaugeAdapter = new LongGaugeAdapter(testMeter, "name", "desc", "unit");

        longGaugeAdapter.record(1L, Map.of("k", 1L));

        ArgumentCaptor<Consumer<ObservableLongMeasurement>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(longGaugeBuilder).buildWithCallback(captor.capture());

        Consumer<ObservableLongMeasurement> value = captor.getValue();
        TestLongMeasurement testLongMeasurement = new TestLongMeasurement();
        value.accept(testLongMeasurement);

        assertThat(testLongMeasurement.value, Matchers.equalTo(1L));
        assertThat(testLongMeasurement.attributes, Matchers.equalTo(Attributes.builder().put("k", 1).build()));
    }

    private static class TestLongMeasurement implements ObservableLongMeasurement {
        long value;
        Attributes attributes;

        @Override
        public void record(long value) {
            this.value = value;
        }

        @Override
        public void record(long value, Attributes attributes) {
            this.value = value;
            this.attributes = attributes;

        }
    }
}
