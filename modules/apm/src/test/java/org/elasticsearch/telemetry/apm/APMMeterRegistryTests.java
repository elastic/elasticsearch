/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.APMMeterService;
import org.elasticsearch.telemetry.apm.internal.TestAPMMeterService;
import org.elasticsearch.telemetry.metric.DoubleAsyncCounter;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.LongAsyncCounter;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class APMMeterRegistryTests extends ESTestCase {
    RecordingOtelMeter testOtel = new RecordingOtelMeter();

    Meter noopOtel = OpenTelemetry.noop().getMeter("noop");

    private Settings TELEMETRY_ENABLED = Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true).build();

    public void testMeterIsSetUponConstruction() {
        // test default
        APMMeterService apmMeter = new APMMeterService(Settings.EMPTY, () -> testOtel, () -> noopOtel);

        Meter meter = apmMeter.getMeterRegistry().getMeter();
        assertThat(meter, sameInstance(noopOtel));

        // test explicitly enabled
        apmMeter = new APMMeterService(TELEMETRY_ENABLED, () -> testOtel, () -> noopOtel);

        meter = apmMeter.getMeterRegistry().getMeter();
        assertThat(meter, sameInstance(testOtel));

        // test explicitly disabled
        var settings = Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), false).build();
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

        DoubleCounter registeredCounter = apmMeter.registerDoubleCounter("es.test.name.total", "desc", "unit");
        DoubleCounter lookedUpCounter = apmMeter.getDoubleCounter("es.test.name.total");

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

    public void testAllInstrumentsSwitchProviders() {
        TestAPMMeterService apmMeter = new TestAPMMeterService(
            Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), false).build(),
            () -> testOtel,
            () -> noopOtel
        );
        APMMeterRegistry registry = apmMeter.getMeterRegistry();

        Supplier<DoubleWithAttributes> doubleObserver = () -> new DoubleWithAttributes(1.5, Collections.emptyMap());
        DoubleCounter dc = registry.registerDoubleCounter("es.test.dc.total", "", "");
        DoubleUpDownCounter dudc = registry.registerDoubleUpDownCounter("es.test.dudc.current", "", "");
        DoubleHistogram dh = registry.registerDoubleHistogram("es.test.dh.histogram", "", "");
        DoubleAsyncCounter dac = registry.registerDoubleAsyncCounter("es.test.dac.total", "", "", doubleObserver);
        DoubleGauge dg = registry.registerDoubleGauge("es.test.dg.current", "", "", doubleObserver);

        Supplier<LongWithAttributes> longObserver = () -> new LongWithAttributes(100, Collections.emptyMap());
        LongCounter lc = registry.registerLongCounter("es.test.lc.total", "", "");
        LongUpDownCounter ludc = registry.registerLongUpDownCounter("es.test.ludc.total", "", "");
        LongHistogram lh = registry.registerLongHistogram("es.test.lh.histogram", "", "");
        LongAsyncCounter lac = registry.registerLongAsyncCounter("es.test.lac.total", "", "", longObserver);
        LongGauge lg = registry.registerLongGauge("es.test.lg.current", "", "", longObserver);

        apmMeter.setEnabled(true);

        testOtel.collectMetrics();
        dc.increment();
        dudc.add(1.0);
        dh.record(1.0);
        lc.increment();
        ludc.add(1);
        lh.record(1);

        hasOneDouble(dc, 1.0d);
        hasOneDouble(dudc, 1.0d);
        hasOneDouble(dh, 1.0d);
        hasOneDouble(dac, 1.5d);
        hasOneDouble(dg, 1.5d);

        hasOneLong(lc, 1L);
        hasOneLong(ludc, 1L);
        hasOneLong(lh, 1L);
        hasOneLong(lac, 100L);
        hasOneLong(lg, 100L);
    }

    private void hasOneDouble(Instrument instrument, double value) {
        List<Measurement> measurements = testOtel.getRecorder().getMeasurements(instrument);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.get(0).getDouble(), equalTo(value));
    }

    private void hasOneLong(Instrument instrument, long value) {
        List<Measurement> measurements = testOtel.getRecorder().getMeasurements(instrument);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.get(0).getLong(), equalTo(value));
    }
}
