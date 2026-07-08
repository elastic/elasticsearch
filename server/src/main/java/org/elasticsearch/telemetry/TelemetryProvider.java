/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry;

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

    Tracer getTracer();

    MeterRegistry getMeterRegistry();

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
        public void attemptFlush() {}
    }
}
