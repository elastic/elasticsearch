/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.telemetry.MetricName;
import org.elasticsearch.telemetry.metric.Counter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.telemetry.apm.settings.APMAgentSettings.APM_ENABLED_SETTING;

public class APMMetric extends AbstractLifecycleComponent implements org.elasticsearch.telemetry.metric.Metric {
    private final Map<MetricName, Counter> counters = ConcurrentCollections.newConcurrentMap();
    private final Map<MetricName, DoubleGauge> doubleGauges = ConcurrentCollections.newConcurrentMap();
    private final Map<MetricName, DoubleHistogram> doubleHistograms = ConcurrentCollections.newConcurrentMap();
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

    public <T> Counter registerCounter(MetricName name, String description, T unit) {
        return counters.compute(name, (k, v) -> {
            if (v != null) {
                throw new IllegalStateException("double gauge [" + name.getRawName() + "] already registered");
            }
            var lazyCounter = new LazyInitializable<>(
                () -> services.get().meter.counterBuilder(name.getRawName()).setDescription(description).setUnit(unit.toString()).build()
            );
            return OtelCounter.build(lazyCounter, name, description, unit);
        });
    }

    @Override

    public <T> DoubleGauge registerDoubleGauge(MetricName name, String description, T unit) {
        return doubleGauges.compute(name, (k, v) -> {
            if (v != null) {
                throw new IllegalStateException("double gauge [" + name.getRawName() + "] already registered");
            }
            var lazyGauge = new LazyInitializable<>(
                () -> services.get().meter.gaugeBuilder(name.getRawName())
                    .setDescription(description)
                    .setUnit(unit.toString())
                    .buildObserver()
            );

            return OtelDoubleGauge.build(lazyGauge, name, description, unit);
        });
    }

    @Override

    public <T> DoubleHistogram registerDoubleHistogram(MetricName name, String description, T unit) {
        return doubleHistograms.compute(name, (k, v) -> {
            if (v != null) {
                throw new IllegalStateException("double histogram [" + name.getRawName() + "] already registered");
            }
            var lazyHistogram = new LazyInitializable<>(
                () -> services.get().meter.histogramBuilder(name.getRawName()).setDescription(description).setUnit(unit.toString()).build()
            );

            return OtelDoubleHistogram.build(lazyHistogram, name, description, unit);
        });
    }

    void createApmServices() {
        assert this.enabled;
        assert this.services.get() == null;

        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            var openTelemetry = GlobalOpenTelemetry.get();
            var meter = openTelemetry.getMeter("elasticsearch");

            this.services.set(new APMServices(meter, openTelemetry));

            Meter myMeter = GlobalOpenTelemetry.getMeter("my_meter");
            LongCounter counter = myMeter.counterBuilder("my_counter").build();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        counter.add(42);
                        Thread.sleep(2000);
                    } catch (Exception e) {

                    }
                }
            }).start();

            return null;
        });
    }

    private void destroyApmServices() {
        this.services.set(null);
    }

}
