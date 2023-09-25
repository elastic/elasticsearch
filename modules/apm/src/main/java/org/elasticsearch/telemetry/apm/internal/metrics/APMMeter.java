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
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.MetricName;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.telemetry.apm.internal.APMAgentSettings.APM_ENABLED_SETTING;

public class APMMeter extends AbstractLifecycleComponent implements org.elasticsearch.telemetry.metric.Metric {
    private static final Meter NOOP = OpenTelemetry.noop().getMeter("noop");
    private final Instruments instruments = new Instruments(NOOP);
    private volatile boolean enabled;
    private final AtomicReference<APMServices> services = new AtomicReference<>();

    record APMServices(Meter meter, OpenTelemetry openTelemetry) {}

    public APMMeter(Settings settings) {
        this.enabled = APM_ENABLED_SETTING.get(settings);
        setupApmServices(this.enabled);
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        setupApmServices(this.enabled);
    }

    private void setupApmServices(boolean enabled) {
        if (enabled) {
            createApmServices();
        } else {
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
        return instruments.registerDoubleCounter(name, description, unit);
    }

    @Override
    public DoubleCounter getDoubleCounter(MetricName name) {
        return instruments.getDoubleCounter(name);
    }

    @Override
    public <T> DoubleUpDownCounter registerDoubleUpDownCounter(MetricName name, String description, T unit) {
        return instruments.registerDoubleUpDownCounter(name, description, unit);
    }

    @Override
    public DoubleUpDownCounter getDoubleUpDownCounter(MetricName name) {
        return instruments.getDoubleUpDownCounter(name);
    }

    @Override
    public <T> DoubleGauge registerDoubleGauge(MetricName name, String description, T unit) {
        return instruments.registerDoubleGauge(name, description, unit);
    }

    @Override
    public DoubleGauge getDoubleGauge(MetricName name) {
        return instruments.getDoubleGauge(name);
    }

    @Override
    public <T> DoubleHistogram registerDoubleHistogram(MetricName name, String description, T unit) {
        return instruments.registerDoubleHistogram(name, description, unit);
    }

    @Override
    public DoubleHistogram getDoubleHistogram(MetricName name) {
        return instruments.getDoubleHistogram(name);
    }

    @Override
    public <T> LongCounter registerLongCounter(MetricName name, String description, T unit) {
        return instruments.registerLongCounter(name, description, unit);
    }

    @Override
    public LongCounter getLongCounter(MetricName name) {
        return instruments.getLongCounter(name);
    }

    @Override
    public <T> LongUpDownCounter registerLongUpDownCounter(MetricName name, String description, T unit) {
        return instruments.registerLongUpDownCounter(name, description, unit);
    }

    @Override
    public LongUpDownCounter getLongUpDownCounter(MetricName name) {
        return instruments.getLongUpDownCounter(name);
    }

    @Override
    public <T> LongGauge registerLongGauge(MetricName name, String description, T unit) {
        return instruments.registerLongGauge(name, description, unit);
    }

    @Override
    public LongGauge getLongGauge(MetricName name) {
        return instruments.getLongGauge(name);
    }

    @Override
    public <T> LongHistogram registerLongHistogram(MetricName name, String description, T unit) {
        return instruments.registerLongHistogram(name, description, unit);
    }

    @Override
    public LongHistogram getLongHistogram(MetricName name) {
        return instruments.getLongHistogram(name);
    }

    void createApmServices() {
        assert this.enabled;
        assert this.services.get() == null;

        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            var openTelemetry = GlobalOpenTelemetry.get();
            var meter = openTelemetry.getMeter("elasticsearch");

            this.services.set(new APMServices(meter, openTelemetry));
            instruments.setProvider(meter);

            return null;
        });
    }

    private void destroyApmServices() {
        instruments.setProvider(NOOP);
        this.services.set(null);
    }
}
