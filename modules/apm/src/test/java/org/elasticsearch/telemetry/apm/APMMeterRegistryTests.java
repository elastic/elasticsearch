/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.APMMeterService;
import org.elasticsearch.telemetry.apm.internal.TestAPMMeterService;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.sameInstance;

public class APMMeterRegistryTests extends ESTestCase {
    Meter testOtel = OpenTelemetry.noop().getMeter("test");

    Meter noopOtel = OpenTelemetry.noop().getMeter("noop");
    private static final Settings TELEMETRY_ENABLED = Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true).build();

    public void testMeterIsSetUponConstruction() {
        // test default
        APMMeterService apmMeter = new APMMeterService(Settings.EMPTY, () -> testOtel, () -> noopOtel);

        Meter meter = apmMeter.getMeterRegistry().getMeter();
        assertThat(meter, sameInstance(noopOtel));

        // test explicitly enabled
        var settings = Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true).build();
        apmMeter = new APMMeterService(settings, () -> testOtel, () -> noopOtel);

        meter = apmMeter.getMeterRegistry().getMeter();
        assertThat(meter, sameInstance(testOtel));

        // test explicitly disabled
        settings = Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true).build();
        apmMeter = new APMMeterService(settings, () -> testOtel, () -> noopOtel);

        meter = apmMeter.getMeterRegistry().getMeter();
        assertThat(meter, sameInstance(noopOtel));
    }

    public void testMeterIsOverridden() {
        TestAPMMeterService apmMeter = new TestAPMMeterService(Settings.EMPTY, () -> testOtel, () -> noopOtel);

        Meter meter = apmMeter.getMeterRegistry().getMeter();
        assertThat(meter, sameInstance(noopOtel));

        apmMeter.setEnabled(true);

        meter = apmMeter.getMeterRegistry().getMeter();
        assertThat(meter, sameInstance(testOtel));
    }

    public void testLookupByName() {
        var apmMeter = new APMMeterService(TELEMETRY_ENABLED, () -> testOtel, () -> noopOtel).getMeterRegistry();

        DoubleCounter registeredCounter = apmMeter.registerDoubleCounter("name", "desc", "unit");
        DoubleCounter lookedUpCounter = apmMeter.getDoubleCounter("name");

        assertThat(lookedUpCounter, sameInstance(registeredCounter));
    }

    public void testNoopIsSetOnStop() {
        APMMeterService apmMeter = new APMMeterService(TELEMETRY_ENABLED, () -> testOtel, () -> noopOtel);
        apmMeter.start();

        Meter meter = apmMeter.getMeterRegistry().getMeter();
        assertThat(meter, sameInstance(testOtel));

        apmMeter.stop();

        meter = apmMeter.getMeterRegistry().getMeter();
        assertThat(meter, sameInstance(noopOtel));
    }

    public void testLongName() {
        APMMeterService apm = new APMMeterService(TELEMETRY_ENABLED);
        LongCounter counter = apm.getMeterRegistry().registerLongCounter("a".repeat(64), "desc", "count");
        LongCounter counter2 = apm.getMeterRegistry().registerLongCounter("a".repeat(256), "desc", "count");
        counter.increment();
        counter2.increment();
    }
}
