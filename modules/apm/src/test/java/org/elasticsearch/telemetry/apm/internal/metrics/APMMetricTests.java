/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.DoubleCounterBuilder;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import junit.framework.TestCase;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.Metric;
import org.elasticsearch.telemetry.metric.MetricName;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class APMMetricTests extends ESTestCase {
    Meter testOtel = OpenTelemetry.noop().getMeter("test");

    Meter noopOtel = OpenTelemetry.noop().getMeter("noop");


    @Test
    public void testMeterIsSetUponConstruction() {
        //test default
        APMMetric apmMetric = new APMMetric(Settings.EMPTY, () -> testOtel, () -> noopOtel);

        Meter meter = apmMetric.getInstruments().getMeter();
        assertThat(meter, Matchers.sameInstance(noopOtel));

        //test explicitly enabled
        var settings = Settings.builder().put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true).build();
        apmMetric = new APMMetric(settings, () -> testOtel, () -> noopOtel);

        meter = apmMetric.getInstruments().getMeter();
        assertThat(meter, Matchers.sameInstance(testOtel));

        //test explicitly disabled
        settings = Settings.builder().put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true).build();
        apmMetric = new APMMetric(settings, () -> testOtel, () -> noopOtel);

        meter = apmMetric.getInstruments().getMeter();
        assertThat(meter, Matchers.sameInstance(noopOtel));
    }


    @Test
    public void testMeterIsOverridden() {
        //test default
        APMMetric apmMetric = new APMMetric(Settings.EMPTY, () -> testOtel, () -> noopOtel);

        Meter meter = apmMetric.getInstruments().getMeter();
        assertThat(meter, Matchers.sameInstance(noopOtel));

        apmMetric.setEnabled(true);

        meter = apmMetric.getInstruments().getMeter();
        assertThat(meter, Matchers.sameInstance(testOtel));
    }


    @Test
    public void testLookupByName() {
        var settings = Settings.builder().put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true).build();

        Metric apmMetric = new APMMetric(settings, () -> testOtel, () -> noopOtel);

        DoubleCounter registeredCounter = apmMetric.registerDoubleCounter(new MetricName("name"), "desc", "unit");
        DoubleCounter lookedUpCounter = apmMetric.getDoubleCounter(new MetricName("name"));

        assertThat(lookedUpCounter, Matchers.sameInstance(registeredCounter));
    }
}
