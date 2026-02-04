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
        this(settings, createOtelMeterSupplier(settings), () -> OpenTelemetry.noop().getMeter("noop"));
    }

    public APMMeterService(Settings settings, Supplier<Meter> otelMeterSupplier, Supplier<Meter> noopMeterSupplier) {
        this(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.get(settings), otelMeterSupplier, noopMeterSupplier);
    }

    public APMMeterService(boolean enabled, Supplier<Meter> otelMeterSupplier, Supplier<Meter> noopMeterSupplier) {
        this.enabled = enabled;
        this.otelMeterSupplier = otelMeterSupplier;
        this.noopMeterSupplier = noopMeterSupplier;
        this.meterRegistry = new APMMeterRegistry(enabled ? otelMeterSupplier.get() : noopMeterSupplier.get());
        if (enabled && otelMeterSupplier instanceof OtelSdkMeterSupplier) {
            SystemMetrics.register(meterRegistry);
        }
    }

    private static Supplier<Meter> createOtelMeterSupplier(Settings settings) {
        if (APMAgentSettings.TELEMETRY_OTEL_METRICS_ENABLED_SETTING.get(settings) == false) {
            return () -> GlobalOpenTelemetry.get().getMeter("elasticsearch");
        }
        return new OtelSdkMeterSupplier(settings);
    }

    public APMMeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    /**
     * @see APMAgentSettings#addClusterSettingsListeners(ClusterService, APMTelemetryProvider)
     */
    void setEnabled(boolean enabled) {
        this.enabled = enabled;
        meterRegistry.setProvider(enabled ? otelMeterSupplier.get() : noopMeterSupplier.get());
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {
        if (otelMeterSupplier instanceof OtelSdkMeterSupplier otelSdk) {
            otelSdk.close();
        }
        meterRegistry.setProvider(noopMeterSupplier.get());
    }

    @Override
    protected void doClose() {}
}
