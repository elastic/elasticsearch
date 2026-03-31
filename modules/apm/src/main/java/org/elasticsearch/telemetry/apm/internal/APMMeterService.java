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
            return new OTelSdkMeterSupplier(settings);
        } else {
            long agentFlushWaitMs = 2 * agentMetricsInterval(settings).millis();
            return new MeterSupplier() {
                @Override
                public Meter get() {
                    // CONFUSION ALERT: When we do `GlobalOpenTelemetry.get()`, we're actually getting an OpenTelemetry
                    // object that routes telemetry to the APM agent; that is, we're still using OTel to report telemetry
                    // from the code, but we're using the APM agent (instead of the OTel SDK) to export it.
                    // That's why this "else" branch, where otelMetricsEnabled is false, is still using OpenTelemetry.

                    return GlobalOpenTelemetry.get().getMeter("elasticsearch");
                }

                @Override
                public void attemptFlushMetrics() {
                    try {
                        // The agent offers no flush API, so we do a best-effort pause that exceeds
                        // the agent reporting interval, making it extremely likely that all telemetry
                        // has been exported.
                        //
                        // Note that the first intake request to the APM server can still be delayed beyond this window:
                        // the APM agent checks for configuration changes only periodically,
                        // so the setting changes we made during initialization don't take effect immediately.

                        LOGGER.info("Waiting {} ms for APM agent to flush metrics", agentFlushWaitMs);
                        Thread.sleep(agentFlushWaitMs);
                    } catch (InterruptedException e) {
                        // Flush is best-effort. We can reestablish the interrupt flag and proceed.
                        Thread.currentThread().interrupt();
                    }
                }
            };
        }
    }

    private static TimeValue agentMetricsInterval(Settings settings) {
        String intervalStr = settings.get("telemetry.agent.metrics_interval");
        if (intervalStr != null && intervalStr.isEmpty() == false) {
            return TimeValue.parseTimeValue(intervalStr, "telemetry.agent.metrics_interval");
        }
        return DEFAULT_AGENT_INTERVAL;
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
