/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.sdk.common.CompletableResultCode;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.apm.APMMeterRegistry;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class APMTelemetryProvider implements TelemetryProvider {
    private final APMTracer apmTracer;
    private final APMMeterService apmMeterService;
    private final APMLoggingService loggingService;
    private final long flushTimeoutMillis;

    public APMTelemetryProvider(Settings settings, Path diskBufferPath) {
        apmMeterService = new APMMeterService(settings, diskBufferPath);
        apmTracer = new APMTracer(settings, apmMeterService::getHealthMeterProvider);
        loggingService = new APMLoggingService(settings);
        flushTimeoutMillis = OtelSdkSettings.TELEMETRY_OTEL_FLUSH_TIMEOUT.get(settings).millis();
    }

    // visible for testing: pre-built service/tracer instances with stubbed suppliers
    public APMTelemetryProvider(
        APMMeterService apmMeterService,
        APMTracer apmTracer,
        APMLoggingService loggingService,
        long flushTimeoutMillis
    ) {
        this.apmMeterService = apmMeterService;
        this.apmTracer = apmTracer;
        this.loggingService = loggingService;
        this.flushTimeoutMillis = flushTimeoutMillis;
    }

    @Override
    public APMTracer getTracer() {
        return apmTracer;
    }

    public APMMeterService getMeterService() {
        return apmMeterService;
    }

    @Override
    public APMMeterRegistry getMeterRegistry() {
        return apmMeterService.getMeterRegistry();
    }

    @Override
    public void attemptFlush() {
        CompletableResultCode metrics = apmMeterService.attemptFlushMetrics();
        CompletableResultCode traces = apmTracer.attemptFlushTraces();
        CompletableResultCode logs = loggingService.forceFlush();
        CompletableResultCode.ofAll(List.of(metrics, traces, logs)).join(flushTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    public APMLoggingService getLoggingService() {
        return loggingService;
    }
}
