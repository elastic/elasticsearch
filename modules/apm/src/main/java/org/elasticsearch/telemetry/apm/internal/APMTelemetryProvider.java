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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.TelemetryLogResourceProvider;
import org.elasticsearch.telemetry.TelemetryLoggingFilterProvider;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.apm.internal.instrumentation.HttpServerInstrumentations;
import org.elasticsearch.telemetry.apm.internal.instrumentation.HttpServerTracing;
import org.elasticsearch.telemetry.apm.internal.instrumentation.RestRequestMetrics;
import org.elasticsearch.telemetry.apm.internal.metrics.spi.MetricReaderProvider;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class APMTelemetryProvider implements TelemetryProvider {
    private final APMTracingService tracingService;
    private final APMMeterService meterService;
    private final APMLoggingService loggingService;
    private final HttpServerInstrumentation httpServerInstrumentation;

    public APMTelemetryProvider(
        ThreadContext threadContext,
        Settings settings,
        Path diskBufferPath,
        Path configDir,
        Collection<TelemetryLoggingFilterProvider> filterProviders,
        TelemetryLogResourceProvider logResourceProvider,
        @Nullable MetricReaderProvider metricReaderProvider
    ) {
        meterService = new APMMeterService(settings, diskBufferPath, metricReaderProvider);
        tracingService = new APMTracingService(threadContext, settings, meterService::getHealthMeterProvider);
        loggingService = new APMLoggingService(
            settings,
            configDir,
            filterProviders,
            logResourceProvider,
            meterService::getHealthMeterProvider
        );
        httpServerInstrumentation = new HttpServerInstrumentations(
            List.of(new HttpServerTracing(tracingService), new RestRequestMetrics(threadContext, meterService.getMeterRegistry()))
        );
    }

    // visible for testing: pre-built service/tracer instances with stubbed suppliers
    public APMTelemetryProvider(
        APMMeterService meterService,
        APMTracingService tracingService,
        APMLoggingService loggingService,
        HttpServerInstrumentation httpServerInstrumentation
    ) {
        this.meterService = meterService;
        this.tracingService = tracingService;
        this.loggingService = loggingService;
        this.httpServerInstrumentation = httpServerInstrumentation;
    }

    @Override
    public Tracer getTracer(String scopeName) {
        return tracingService.tracer(scopeName);
    }

    @Override
    public MeterRegistry getMeterRegistry() {
        return meterService.getMeterRegistry();
    }

    @Override
    public HttpServerInstrumentation getHttpServerInstrumentation() {
        return httpServerInstrumentation;
    }

    @Override
    public void attemptFlush() {
        CompletableResultCode metrics = meterService.attemptFlushMetrics();
        CompletableResultCode traces = tracingService.attemptFlushTraces();
        CompletableResultCode logs = loggingService.forceFlush();
        CompletableResultCode.ofAll(List.of(metrics, traces, logs))
            .join(OtelSdkSettings.OTEL_EXPORT_FLUSH_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
    }

    public APMTracingService getTracingService() {
        return tracingService;
    }

    public APMMeterService getMeterService() {
        return meterService;
    }

    public APMLoggingService getLoggingService() {
        return loggingService;
    }
}
