/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.telemetry.apm.APMMeterRegistry;
import org.elasticsearch.telemetry.apm.internal.export.MeterSupplier;
import org.elasticsearch.telemetry.apm.internal.export.agent.AgentExportMeterSupplier;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkExportMeterSupplier;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;

public class APMMeterService extends AbstractLifecycleComponent {

    private static final Logger LOGGER = LogManager.getLogger(APMMeterService.class);

    private final APMMeterRegistry meterRegistry;
    private final MeterSupplier otelMeterSupplier;
    private final MeterSupplier noopMeterSupplier;

    protected volatile boolean enabled;

    public APMMeterService(Settings settings) {
        this(settings, createOtelMeterSupplier(settings), new NoOpMeterSupplier());
    }

    public APMMeterService(Settings settings, MeterSupplier otelMeterSupplier, MeterSupplier noopMeterSupplier) {
        this.enabled = APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.get(settings);
        this.otelMeterSupplier = otelMeterSupplier;
        this.noopMeterSupplier = noopMeterSupplier;
        this.meterRegistry = new APMMeterRegistry(enabled ? otelMeterSupplier.get() : noopMeterSupplier.get());
    }

    private static MeterSupplier createOtelMeterSupplier(Settings settings) {
        boolean otelMetricsEnabled = Booleans.parseBoolean(System.getProperty(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY, "false"));
        if (otelMetricsEnabled) {
            return new OtelSdkExportMeterSupplier(settings);
        } else {
            return new AgentExportMeterSupplier(settings);
        }
    }

    public APMMeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    /**
     * Export buffered metrics on a best-effort basis.
     * <p>
     * For OpenTelemetry SDK metrics, pushes buffered data to the exporter. For Elastic APM agent metrics,
     * sleeps for {@code 2 * telemetry.agent.metrics_interval} because the agent has no
     * programmatic flush; observable export (e.g. first HTTP to {@code telemetry.agent.server_url}) may still
     * take substantially longer than this sleep.
     */
    public void attemptFlushMetrics() {
        if (enabled) {
            otelMeterSupplier.attemptFlushMetrics();
        }
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
        if (enabled) {
            try {
                otelMeterSupplier.attemptFlushMetrics();
            } catch (Exception e) {
                LOGGER.warn("Exception flushing OTel MeterSupplier", e);
            }
        }
        try {
            otelMeterSupplier.close();
        } catch (Exception e) {
            LOGGER.warn("Exception closing OTel MeterSupplier", e);
        }
        meterRegistry.setProvider(noopMeterSupplier.get());
    }

    @Override
    protected void doClose() {}

    private static final class NoOpMeterSupplier implements MeterSupplier {
        @Override
        public Meter get() {
            return OpenTelemetry.noop().getMeter("noop");
        }

        @Override
        public void attemptFlushMetrics() {
            // No-op
        }
    }
}
