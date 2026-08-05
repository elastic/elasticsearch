/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;

public interface TelemetryProvider {

    String OTEL_METRICS_ENABLED_SYSTEM_PROPERTY = "telemetry.otel.metrics.enabled";

    /**
     * JVM system property that activates the OTel SDK trace export path.
     * Set via {@code config/jvm.options} (or {@code -D} on the command line); not settable via
     * {@code elasticsearch.yml} or the cluster settings API.
     */
    String OTEL_TRACES_ENABLED_SYSTEM_PROPERTY = "telemetry.otel.traces.enabled";

    /**
     * Resolves the interval at which node and indices metrics are collected to use for {@code NodeMetrics} cached.
     * <p>
     * When the OTel SDK metrics export path is active (see {@link #OTEL_METRICS_ENABLED_SYSTEM_PROPERTY}), the interval
     * tracks the SDK export interval ({@code telemetry.export.interval}, falling back to
     * {@code telemetry.agent.metrics_interval} with a 60s default) so metrics are refreshed in step with exports.
     * Otherwise, it uses the legacy APM agent interval ({@code telemetry.agent.metrics_interval}, defaulting to 10s).
     */
    static TimeValue getMetricsInterval(Settings settings) {
        boolean otelMetricsEnabled = Booleans.parseBoolean(System.getProperty(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY, "false"));
        return otelMetricsEnabled
            ? settings.getAsTime(
                "telemetry.export.interval",
                settings.getAsTime("telemetry.agent.metrics_interval", TimeValue.timeValueSeconds(60))
            )
            : settings.getAsTime("telemetry.agent.metrics_interval", TimeValue.timeValueSeconds(10));
    }

    Tracer getTracer();

    MeterRegistry getMeterRegistry();

    HttpServerInstrumentation getHttpServerInstrumentation();

    /**
     * Forces any buffered telemetry (metrics, traces, and log records) to be exported immediately.
     * Implementations should flush all signals concurrently where possible and bound the wait to
     * an appropriate timeout.
     */
    void attemptFlush();

    TelemetryProvider NOOP = new NoopTelemetryProvider();

    class NoopTelemetryProvider implements TelemetryProvider {

        @Override
        public Tracer getTracer() {
            return Tracer.NOOP;
        }

        @Override
        public MeterRegistry getMeterRegistry() {
            return MeterRegistry.NOOP;
        }

        @Override
        public HttpServerInstrumentation getHttpServerInstrumentation() {
            return HttpServerInstrumentation.NOOP;
        }

        @Override
        public void attemptFlush() {}
    }
}
