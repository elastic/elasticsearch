/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.APMMeterRegistry;

import java.util.function.Supplier;

public class APMMeterService extends AbstractLifecycleComponent {
    private final APMMeterRegistry meterRegistry;

    private final Supplier<Meter> otelMeterSupplier;
    private final Supplier<Meter> noopMeterSupplier;

    protected volatile boolean enabled;

    public APMMeterService(Settings settings) {
        this(settings, APMMeterService.otelMeter(), APMMeterService.noopMeter());
    }

    public APMMeterService(Settings settings, Supplier<Meter> otelMeterSupplier, Supplier<Meter> noopMeterSupplier) {
        this(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.get(settings), otelMeterSupplier, noopMeterSupplier);
    }

    public APMMeterService(boolean enabled, Supplier<Meter> otelMeterSupplier, Supplier<Meter> noopMeterSupplier) {
        this.enabled = enabled;
        this.otelMeterSupplier = otelMeterSupplier;
        this.noopMeterSupplier = noopMeterSupplier;
        this.meterRegistry = new APMMeterRegistry(enabled ? createOtelMeter() : createNoopMeter());
    }

    public APMMeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    /**
     * @see APMAgentSettings#addClusterSettingsListeners(ClusterService, APMTelemetryProvider)
     */
    void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            meterRegistry.setProvider(createOtelMeter());
        } else {
            meterRegistry.setProvider(createNoopMeter());
        }
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {
        meterRegistry.setProvider(createNoopMeter());
    }

    @Override
    protected void doClose() {}

    protected Meter createOtelMeter() {
        assert this.enabled;
        return otelMeterSupplier.get();
    }

    protected Meter createNoopMeter() {
        return noopMeterSupplier.get();
    }

    protected static Supplier<Meter> noopMeter() {
        return () -> OpenTelemetry.noop().getMeter("noop");
    }

    // to be used within doPrivileged block
    private static Supplier<Meter> otelMeter() {
        var openTelemetry = GlobalOpenTelemetry.get();
        var meter = openTelemetry.getMeter("elasticsearch");
        return () -> meter;
    }
}
