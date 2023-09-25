/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

public class APMMeterTests extends ESTestCase {
    Meter testOtel = OpenTelemetry.noop().getMeter("test");

    Meter noopOtel = OpenTelemetry.noop().getMeter("noop");

    public void testMeterIsSetUponConstruction() {
        // test default
        APMMeter apmMeter = new APMMeter(Settings.EMPTY, () -> testOtel, () -> noopOtel);

        Meter meter = apmMeter.getInstruments().getMeter();
        assertThat(meter, Matchers.sameInstance(noopOtel));

        // test explicitly enabled
        var settings = Settings.builder().put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true).build();
        apmMeter = new APMMeter(settings, () -> testOtel, () -> noopOtel);

        meter = apmMeter.getInstruments().getMeter();
        assertThat(meter, Matchers.sameInstance(testOtel));

        // test explicitly disabled
        settings = Settings.builder().put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true).build();
        apmMeter = new APMMeter(settings, () -> testOtel, () -> noopOtel);

        meter = apmMeter.getInstruments().getMeter();
        assertThat(meter, Matchers.sameInstance(noopOtel));
    }

    public void testMeterIsOverridden() {
        // test default
        APMMeter apmMeter = new APMMeter(Settings.EMPTY, () -> testOtel, () -> noopOtel);

        Meter meter = apmMeter.getInstruments().getMeter();
        assertThat(meter, Matchers.sameInstance(noopOtel));

        apmMeter.setEnabled(true);

        meter = apmMeter.getInstruments().getMeter();
        assertThat(meter, Matchers.sameInstance(testOtel));
    }

    public void testLookupByName() {
        var settings = Settings.builder().put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true).build();

        var apmMeter = new APMMeter(settings, () -> testOtel, () -> noopOtel);

        DoubleCounter registeredCounter = apmMeter.registerDoubleCounter("name", "desc", "unit");
        DoubleCounter lookedUpCounter = apmMeter.getDoubleCounter("name");

        assertThat(lookedUpCounter, Matchers.sameInstance(registeredCounter));
    }
}
