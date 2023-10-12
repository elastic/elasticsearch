/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.LongGaugeBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
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

public class GaugeAdapterTests extends ESTestCase {
    Meter testMeter = Mockito.mock(Meter.class);
    LongGaugeBuilder longGaugeBuilder = Mockito.mock(LongGaugeBuilder.class);
    DoubleGaugeBuilder mockDoubleGaugeBuilder = Mockito.mock(DoubleGaugeBuilder.class);

    @Before
    public void init() {
        when(longGaugeBuilder.setDescription(Mockito.anyString())).thenReturn(longGaugeBuilder);
        when(longGaugeBuilder.setUnit(Mockito.anyString())).thenReturn(longGaugeBuilder);


        when(mockDoubleGaugeBuilder.ofLongs()).thenReturn(longGaugeBuilder);
        when(mockDoubleGaugeBuilder.setUnit(Mockito.anyString())).thenReturn(mockDoubleGaugeBuilder);
        when(mockDoubleGaugeBuilder.setDescription(Mockito.anyString())).thenReturn(mockDoubleGaugeBuilder);
        when(testMeter.gaugeBuilder(anyString())).thenReturn(mockDoubleGaugeBuilder);
    }

    // testing that a value reported is then used in a callback
    @SuppressWarnings("unchecked")
    public void testLongGaugeRecord() {
        LongGaugeAdapter longGaugeAdapter = new LongGaugeAdapter(testMeter, "name", "desc", "unit");

        // recording a value
        longGaugeAdapter.record(1L, Map.of("k", 1L));

        // upon metric export, the consumer will be called
        ArgumentCaptor<Consumer<ObservableLongMeasurement>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(longGaugeBuilder).buildWithCallback(captor.capture());

        Consumer<ObservableLongMeasurement> value = captor.getValue();
        // making sure that a consumer will fetch the value passed down upon recording of a value
        TestLongMeasurement testLongMeasurement = new TestLongMeasurement();
        value.accept(testLongMeasurement);

        assertThat(testLongMeasurement.value, Matchers.equalTo(1L));
        assertThat(testLongMeasurement.attributes, Matchers.equalTo(Attributes.builder().put("k", 1).build()));
    }

    // testing that a value reported is then used in a callback
    @SuppressWarnings("unchecked")
    public void testDoubleGaugeRecord() {
        DoubleGaugeAdapter doubleGaugeAdapter = new DoubleGaugeAdapter(testMeter, "name", "desc", "unit");

        // recording a value
        doubleGaugeAdapter.record(1.0, Map.of("k", 1.0));

        // upon metric export, the consumer will be called
        ArgumentCaptor<Consumer<ObservableDoubleMeasurement>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockDoubleGaugeBuilder).buildWithCallback(captor.capture());

        Consumer<ObservableDoubleMeasurement> value = captor.getValue();
        // making sure that a consumer will fetch the value passed down upon recording of a value
        TestDoubleMeasurement testLongMeasurement = new TestDoubleMeasurement();
        value.accept(testLongMeasurement);

        assertThat(testLongMeasurement.value, Matchers.equalTo(1.0));
        assertThat(testLongMeasurement.attributes, Matchers.equalTo(Attributes.builder().put("k", 1.0).build()));
    }

    private static class TestDoubleMeasurement implements ObservableDoubleMeasurement {
        double value;
        Attributes attributes;

        @Override
        public void record(double value) {
            this.value = value;
        }

        @Override
        public void record(double value, Attributes attributes) {
            this.value = value;
            this.attributes = attributes;

        }
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
