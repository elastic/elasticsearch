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

    Tracer getTracer();

    MeterRegistry getMeterRegistry();

    /**
     * Ensures buffered metrics are exported. Implementations should flush the meter provider they own
     * (e.g. OTel SdkMeterProvider) or wait for the next Elastic APM Java agent export cycle.
     * <p>
     * When metrics are backed by the Elastic APM agent, there is no flush API: the implementation only waits
     * a bounded interval derived from {@code telemetry.agent.metrics_interval}. The first HTTP request to the
     * configured APM server can still arrive much later (agent reporter scheduling), so callers that need
     * observable export must allow additional wall-clock time beyond this method.
     */
    void attemptFlushMetrics();

    /**
     * Ensures buffered traces are exported. Implementations should flush the tracer provider they own
     * (e.g. OTel SdkTracerProvider) or wait for the next agent export cycle.
     */
    void attemptFlushTraces();

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
        public void attemptFlushMetrics() {}

        @Override
        public void attemptFlushTraces() {}
    }
}
