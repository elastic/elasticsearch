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
import io.opentelemetry.sdk.common.CompletableResultCode;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.TelemetryLogResourceProvider;
import org.elasticsearch.telemetry.TelemetryLoggingFilterProvider;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.apm.internal.instrumentation.APMHttpServerInstrumentation;
import org.elasticsearch.telemetry.apm.internal.metrics.APMMeterRegistry;
import org.elasticsearch.telemetry.apm.internal.metrics.spi.MetricReaderProvider;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracingService;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class APMTelemetryProvider implements TelemetryProvider {
    private final APMTracingService tracingService;
    private final APMMeterService apmMeterService;
    private final APMLoggingService loggingService;
    private final APMHttpServerInstrumentation apmHttpServerInstrumentation;

    public APMTelemetryProvider(
        Settings settings,
        Path diskBufferPath,
        Path configDir,
        Collection<TelemetryLoggingFilterProvider> filterProviders,
        TelemetryLogResourceProvider logResourceProvider,
        @Nullable MetricReaderProvider metricReaderProvider
    ) {
        apmMeterService = new APMMeterService(settings, diskBufferPath, metricReaderProvider);
        tracingService = new APMTracingService(settings, apmMeterService::getHealthMeterProvider);
        loggingService = new APMLoggingService(settings, configDir, filterProviders, logResourceProvider);
        apmHttpServerInstrumentation = new APMHttpServerInstrumentation(tracingService.getOpenTelemetry());
    }

    // visible for testing: pre-built service/tracer instances with stubbed suppliers
    public APMTelemetryProvider(APMMeterService apmMeterService, APMTracingService tracingService, APMLoggingService loggingService) {
        this.apmMeterService = apmMeterService;
        this.tracingService = tracingService;
        this.loggingService = loggingService;
        apmHttpServerInstrumentation = new APMHttpServerInstrumentation(tracingService.getOpenTelemetry());
    }

    @Override
    public OpenTelemetry getOpenTelemetry() {
        return tracingService.getOpenTelemetry();
    }

    public APMTracingService getTracingService() {
        return tracingService;
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
        CompletableResultCode traces = tracingService.attemptFlushTraces();
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
