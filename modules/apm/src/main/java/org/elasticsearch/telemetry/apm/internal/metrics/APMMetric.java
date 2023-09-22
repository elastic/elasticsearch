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
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.telemetry.apm.internal.APMAgentSettings.APM_ENABLED_SETTING;

public class APMMetric extends AbstractLifecycleComponent implements org.elasticsearch.telemetry.metric.Metric {
    private final Instruments instruments = new Instruments();
    private volatile boolean enabled;
    private AtomicReference<APMServices> services = new AtomicReference<>();

    record APMServices(Meter meter, OpenTelemetry openTelemetry) {}

    // TODO remove duplication between APMTracer and APMMetric. enabled, create apm services etc
    public APMMetric(Settings settings) {
        this.enabled = APM_ENABLED_SETTING.get(settings);
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
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
    public <T> DoubleUpDownCounter registerDoubleUpDownCounter(MetricName name, String description, T unit) {
        return instruments.registerDoubleUpDownCounter(name, description, unit);
    }

    @Override
    public <T> DoubleGauge registerDoubleGauge(MetricName name, String description, T unit) {
        return instruments.registerDoubleGauge(name, description, unit);
    }

    @Override
    public <T> DoubleHistogram registerDoubleHistogram(MetricName name, String description, T unit) {
        return instruments.registerDoubleHistogram(name, description, unit);
    }

    @Override
    public <T> LongCounter registerLongCounter(MetricName name, String description, T unit) {
        return instruments.registerLongCounter(name, description, unit);
    }

    @Override
    public <T> LongUpDownCounter registerLongUpDownCounter(MetricName name, String description, T unit) {
        return instruments.registerLongUpDownCounter(name, description, unit);
    }

    @Override
    public <T> LongGauge registerLongGauge(MetricName name, String description, T unit) {
        return instruments.registerLongGauge(name, description, unit);
    }

    @Override
    public <T> LongHistogram registerLongHistogram(MetricName name, String description, T unit) {
        return instruments.registerLongHistogram(name, description, unit);
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
        // TODO(stu): where do we want this?
        instruments.setProvider(new LoggingMeterProvider());
        this.services.set(null);
    }
}
