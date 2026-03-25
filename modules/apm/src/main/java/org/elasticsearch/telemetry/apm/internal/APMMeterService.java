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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.apm.APMMeterRegistry;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;

public class APMMeterService extends AbstractLifecycleComponent {

    private static final Logger LOGGER = LogManager.getLogger(APMMeterService.class);

    /**
     * Time to wait for the APM agent to export telemetry if we don't have access to the settings to check.
     */
    public static final TimeValue DEFAULT_AGENT_INTERVAL = TimeValue.timeValueSeconds(10);

    private final APMMeterRegistry meterRegistry;
    private final MeterSupplier otelMeterSupplier;
    private final MeterSupplier noopMeterSupplier;

    /**
     * Bounded wait when metrics are backed by the Elastic APM agent: {@code 2 * metrics_interval}.
     * The agent offers no flush API, so this is only a best-effort pause; the first intake request to the
     * APM server can still be delayed well beyond this window.
     */
    private final long agentFlushWaitMs;

    protected volatile boolean enabled;

    public APMMeterService(Settings settings) {
        this(settings, createOtelMeterSupplier(settings), () -> OpenTelemetry.noop().getMeter("noop"));
    }

    public APMMeterService(Settings settings, MeterSupplier otelMeterSupplier, MeterSupplier noopMeterSupplier) {
        this(
            APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.get(settings),
            otelMeterSupplier,
            noopMeterSupplier,
            2 * agentMetricsInterval(settings).millis()
        );
    }

    private APMMeterService(boolean enabled, MeterSupplier otelMeterSupplier, MeterSupplier noopMeterSupplier, long agentFlushWaitMs) {
        this.enabled = enabled;
        this.otelMeterSupplier = otelMeterSupplier;
        this.noopMeterSupplier = noopMeterSupplier;
        this.agentFlushWaitMs = agentFlushWaitMs;
        this.meterRegistry = new APMMeterRegistry(enabled ? otelMeterSupplier.get() : noopMeterSupplier.get());
    }

    private static TimeValue agentMetricsInterval(Settings settings) {
        String intervalStr = settings.get("telemetry.agent.metrics_interval");
        if (intervalStr != null && intervalStr.isEmpty() == false) {
            return TimeValue.parseTimeValue(intervalStr, "telemetry.agent.metrics_interval");
        }
        return DEFAULT_AGENT_INTERVAL;
    }

    private static MeterSupplier createOtelMeterSupplier(Settings settings) {
        if (Booleans.parseBoolean(System.getProperty(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY, "false")) == false) {
            return () -> GlobalOpenTelemetry.get().getMeter("elasticsearch");
        }
        return new OTelSdkMeterSupplier(settings);
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
        if (enabled == false) {
            return;
        }
        if (otelMeterSupplier instanceof OTelSdkMeterSupplier otelSupplier) {
            otelSupplier.attemptFlushMetrics();
        } else {
            // This side covers the case where the APM agent has installed a meter supplier.
            // The APM agent offers no way to flush, so we simply wait.
            try {
                LOGGER.info("Waiting {} ms for APM agent to flush metrics", agentFlushWaitMs);
                Thread.sleep(agentFlushWaitMs);
            } catch (InterruptedException e) {
                // Flush is best-effort. We can reestablish the interrupt flag and proceed.
                Thread.currentThread().interrupt();
            }
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
        try {
            otelMeterSupplier.close();
        } catch (Exception e) {
            LOGGER.warn("Exception closing OTel MeterSupplier", e);
        }
        meterRegistry.setProvider(noopMeterSupplier.get());
    }

    @Override
    protected void doClose() {}
}
