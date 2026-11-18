/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporterBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InternalTelemetryVersion;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * {@link TraceSupplier} that exports spans via OTLP/gRPC using its own {@link SdkTracerProvider}.
 */
public class OtelSdkExportTracerSupplier implements TraceSupplier {

    private static final Logger logger = LogManager.getLogger(OtelSdkExportTracerSupplier.class);

    private final Settings settings;
    private final Supplier<MeterProvider> meterProvider;
    final String authHeader;
    private final Object mutex = new Object();
    @Nullable
    private SdkTracerProvider sdkTracerProvider; // guarded by mutex
    @Nullable
    private volatile TracerProvider tracerProvider;

    public OtelSdkExportTracerSupplier(Settings settings, Supplier<MeterProvider> meterProvider) {
        this.settings = settings;
        this.meterProvider = meterProvider;
        this.authHeader = OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings);
    }

    @Override
    public TracerProvider get() {
        var tracerProvider = this.tracerProvider;
        if (tracerProvider != null) {
            return tracerProvider;
        }
        synchronized (mutex) {
            tracerProvider = this.tracerProvider;
            if (tracerProvider != null) {
                return tracerProvider;
            }

            if (sdkTracerProvider == null) {
                sdkTracerProvider = createSdkTracerProvider();
            }
            tracerProvider = sdkTracerProvider == null ? TracerProvider.noop() : sdkTracerProvider;
            this.tracerProvider = tracerProvider;
        }
        return tracerProvider;
    }

    @Override
    public CompletableResultCode attemptFlushTraces() {
        SdkTracerProvider sdkTracerProvider;
        synchronized (mutex) {
            sdkTracerProvider = this.sdkTracerProvider;
        }
        return sdkTracerProvider == null ? CompletableResultCode.ofSuccess() : sdkTracerProvider.forceFlush();
    }

    @Override
    public void close() {
        synchronized (mutex) {
            tracerProvider = TracerProvider.noop();
            if (sdkTracerProvider != null) {
                sdkTracerProvider.close();
                sdkTracerProvider = null;
            }
        }
    }

    @Nullable
    private SdkTracerProvider createSdkTracerProvider() {
        String endpoint = OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isEmpty()) {
            logger.warn("[telemetry.export.endpoint] is not configured; trace export is disabled");
            return null;
        }

        TimeValue interval = OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL.get(settings);
        int maxQueueSize = OtelSdkSettings.TELEMETRY_TRACING_MAX_QUEUE_SIZE.get(settings);
        int maxExportBatchSize = OtelSdkSettings.TELEMETRY_TRACING_MAX_BATCH_SIZE.get(settings);

        // InternalTelemetryVersion is @Internal but is the only way to opt into stable SemConv names in 1.62.0.
        OtlpGrpcSpanExporterBuilder builder = OtlpGrpcSpanExporter.builder()
            .setEndpoint(endpoint)
            .setMeterProvider(meterProvider)
            .setInternalTelemetryVersion(InternalTelemetryVersion.LATEST)
            .setTimeout(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.get(settings).toDuration())
            .setConnectTimeout(OtelSdkSettings.TELEMETRY_EXPORT_CONNECT_TIMEOUT.get(settings).toDuration())
            .setRetryPolicy(OtelSdkSettings.OTLP_RETRY_POLICY);
        if (authHeader != null) {
            builder.addHeader("Authorization", authHeader);
        }
        OtelSdkExportMeterSupplier.configureTls(settings, builder::setSslContext);
        OtlpGrpcSpanExporter exporter = builder.build();

        BatchSpanProcessor processor = BatchSpanProcessor.builder(exporter)
            .setMeterProvider(meterProvider)
            .setInternalTelemetryVersion(InternalTelemetryVersion.LATEST)
            .setScheduleDelay(interval.millis(), TimeUnit.MILLISECONDS)
            .setMaxQueueSize(maxQueueSize)
            .setMaxExportBatchSize(maxExportBatchSize)
            .build();

        return SdkTracerProvider.builder()
            .setResource(OtelSdkResource.get(settings))
            .setSampler(new ElasticTracestateSampler(settings))
            .addSpanProcessor(processor)
            .build();
    }
}
