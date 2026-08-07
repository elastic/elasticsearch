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
import org.elasticsearch.telemetry.apm.internal.instrumentation.APMHttpServerInstrumentation;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class APMTelemetryProvider implements TelemetryProvider {
    private final APMTracer apmTracer;
    private final APMMeterService apmMeterService;
    private final APMLoggingService loggingService;
    private final APMHttpServerInstrumentation apmHttpServerInstrumentation;

    public APMTelemetryProvider(Settings settings, Path diskBufferPath, Path configDir) {
        apmMeterService = new APMMeterService(settings, diskBufferPath);
        apmTracer = new APMTracer(settings, apmMeterService::getHealthMeterProvider);
        loggingService = new APMLoggingService(settings, configDir);
        apmHttpServerInstrumentation = new APMHttpServerInstrumentation(apmTracer);
    }

    // visible for testing: pre-built service/tracer instances with stubbed suppliers
    public APMTelemetryProvider(APMMeterService apmMeterService, APMTracer apmTracer, APMLoggingService loggingService) {
        this.apmMeterService = apmMeterService;
        this.apmTracer = apmTracer;
        this.loggingService = loggingService;
        apmHttpServerInstrumentation = new APMHttpServerInstrumentation(apmTracer);
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
    public HttpServerInstrumentation getHttpServerInstrumentation() {
        return apmHttpServerInstrumentation;
    }

    @Override
    public void attemptFlush() {
        CompletableResultCode metrics = apmMeterService.attemptFlushMetrics();
        CompletableResultCode traces = apmTracer.attemptFlushTraces();
        CompletableResultCode logs = loggingService.forceFlush();
        CompletableResultCode.ofAll(List.of(metrics, traces, logs))
            .join(OtelSdkSettings.OTEL_EXPORT_FLUSH_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
    }

    public void initCertReload(ResourceWatcherService resourceWatcher) {
        loggingService.initCertReload(resourceWatcher);
    }

    public APMLoggingService getLoggingService() {
        return loggingService;
    }
}
