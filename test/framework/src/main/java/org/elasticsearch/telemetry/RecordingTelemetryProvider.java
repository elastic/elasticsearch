/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.telemetry.tracing.Tracer;

/**
 * A {@link TelemetryProvider} for tests that records the telemetry so that it can be asserted on. Only supports recording metrics for now.
 */
public class RecordingTelemetryProvider implements TelemetryProvider {

    private final Tracer tracer = Tracer.noop();
    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();

    @Override
    public Tracer getTracer(String scopeName) {
        return tracer;
    }

    @Override
    public RecordingMeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    @Override
    public HttpServerInstrumentation getHttpServerInstrumentation() {
        return HttpServerInstrumentation.NOOP;
    }

    @Override
    public void attemptFlush() {}
}
