/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.telemetry.MetricName;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.telemetry.apm.internal.APMAgentSettings.APM_ENABLED_SETTING;

public class APMMetric extends AbstractLifecycleComponent implements org.elasticsearch.telemetry.metric.Metric {

    private final InstrumentRegistrar<DoubleCounter> doubleCounters = new InstrumentRegistrar<>();
    private final InstrumentRegistrar<DoubleUpDownCounter> doubleUpDownCounters = new InstrumentRegistrar<>();
    private final InstrumentRegistrar<DoubleGauge> doubleGauges = new InstrumentRegistrar<>();
    private final InstrumentRegistrar<DoubleHistogram> doubleHistograms = new InstrumentRegistrar<>();
    private final InstrumentRegistrar<LongCounter> longCounters = new InstrumentRegistrar<>();
    private final InstrumentRegistrar<LongUpDownCounter> longUpDownCounters = new InstrumentRegistrar<>();
    private final InstrumentRegistrar<LongGauge> longGauges = new InstrumentRegistrar<>();
    private final InstrumentRegistrar<LongHistogram> longHistograms = new InstrumentRegistrar<>();
    private volatile boolean enabled;
    private AtomicReference<APMServices> services = new AtomicReference<>();

    private final List<SwitchableInstrument> allRegisteredInstruments = new ArrayList<>();

    record APMServices(Meter meter, OpenTelemetry openTelemetry) {}

    // TODO remove duplication between APMTracer and APMMetric. enabled, create apm services etc
    public APMMetric(Settings settings) {
        this.enabled = APM_ENABLED_SETTING.get(settings);
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            createApmServices();
            allRegisteredInstruments.forEach(si -> si.setEnabled(true));
        } else {
            allRegisteredInstruments.forEach(si -> si.setEnabled(false));
            destroyApmServices();
        }

    }

    @Override
    protected void doStart() {
        if (enabled) {
            createApmServices();
        }
    }

    @Override
    protected void doStop() {
        destroyApmServices();
    }

    @Override
    protected void doClose() {}

    @Override
    public <T> DoubleCounter registerDoubleCounter(MetricName name, String description, T unit) {
        var counter = OtelDoubleCounter.build(()->services.get().meter, name, description, unit);
        doubleCounters.register(counter);
        allRegisteredInstruments.add(counter);
        return counter;
    }

    @Override
    public <T> DoubleUpDownCounter registerDoubleUpDownCounter(MetricName name, String description, T unit) {
        var counter = OtelDoubleUpDownCounter.build(services.get().meter, name, description, unit);
        doubleUpDownCounters.register(counter);
        allRegisteredInstruments.add(counter);

        return counter;
    }

    @Override
    public <T> DoubleGauge registerDoubleGauge(MetricName name, String description, T unit) {
        var gauge = OtelDoubleGauge.build(services.get().meter, name, description, unit);
        doubleGauges.register(gauge);
        allRegisteredInstruments.add(gauge);

        return gauge;
    }

    @Override
    public <T> DoubleHistogram registerDoubleHistogram(MetricName name, String description, T unit) {
        var histogram = OtelDoubleHistogram.build(services.get().meter, name, description, unit);
        doubleHistograms.register(histogram);
        allRegisteredInstruments.add(histogram);
        return histogram;
    }

    @Override
    public <T> LongCounter registerLongCounter(MetricName name, String description, T unit) {
        var lazyCounter = new LazyInitializable<>(
            () -> services.get().meter.counterBuilder(name.getRawName()).setDescription(description).setUnit(unit.toString()).build()
        );
        var counter = OtelLongCounter.build(lazyCounter,services.get().meter, name, description, unit);
        longCounters.register(counter);
        allRegisteredInstruments.add(counter);

        return counter;
    }

    @Override
    public <T> LongUpDownCounter registerLongUpDownCounter(MetricName name, String description, T unit) {

        var lazyCounter = new LazyInitializable<>(
            () -> services.get().meter.upDownCounterBuilder(name.getRawName()).setDescription(description).setUnit(unit.toString()).build()
        );
        var counter = OtelLongUpDownCounter.build(lazyCounter,services.get().meter, name, description, unit);
        longUpDownCounters.register(counter);
        allRegisteredInstruments.add(counter);

        return counter;
    }

    @Override
    public <T> LongGauge registerLongGauge(MetricName name, String description, T unit) {

        var lazyGauge = new LazyInitializable<>(
            () -> services.get().meter.gaugeBuilder(name.getRawName())
                .ofLongs()
                .setDescription(description)
                .setUnit(unit.toString())
                .buildObserver()
        );

        var gauge = OtelLongGauge.build(lazyGauge, services.get().meter, name, description, unit);
        longGauges.register(gauge);
        allRegisteredInstruments.add(gauge);

        return gauge;
    }

    @Override
    public <T> LongHistogram registerLongHistogram(MetricName name, String description, T unit) {

        var lazyHistogram = new LazyInitializable<>(
            () -> services.get().meter.histogramBuilder(name.getRawName())
                .ofLongs()
                .setDescription(description)
                .setUnit(unit.toString())
                .build()
        );

        var histogram = OtelLongHistogram.build(lazyHistogram,services.get().meter, name, description, unit);
        longHistograms.register(histogram);
        allRegisteredInstruments.add(histogram);

        return histogram;
    }

    void createApmServices() {
        assert this.enabled;
        assert this.services.get() == null;

        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            var openTelemetry = GlobalOpenTelemetry.get();
            var meter = openTelemetry.getMeter("elasticsearch");

            this.services.set(new APMServices(meter, openTelemetry));
            return null;
        });
    }

    private void destroyApmServices() {
        this.services.set(null);
    }

}
