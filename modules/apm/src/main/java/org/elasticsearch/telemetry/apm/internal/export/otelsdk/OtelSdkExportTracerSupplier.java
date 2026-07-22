/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InternalTelemetryVersion;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_TRACES_ENABLED_SYSTEM_PROPERTY;

/**
 * {@link TraceSupplier} that exports spans via OTLP/gRPC using its own {@link SdkTracerProvider},
 * used when {@code telemetry.otel.traces.enabled=true} is set as a JVM system property.
 */
public class OtelSdkExportTracerSupplier implements TraceSupplier {

    private final Settings settings;
    private final Supplier<MeterProvider> meterProvider;
    private final Object mutex = new Object();
    private volatile OpenTelemetrySdk openTelemetrySdk;

    public OtelSdkExportTracerSupplier(Settings settings, Supplier<MeterProvider> meterProvider) {
        this.settings = settings;
        this.meterProvider = meterProvider;
    }

    @Override
    public OpenTelemetry get() {
        synchronized (mutex) {
            if (openTelemetrySdk == null) {
                openTelemetrySdk = createOpenTelemetrySdk();
            }
            return openTelemetrySdk;
        }
    }

    @Override
    public CompletableResultCode attemptFlushTraces() {
        OpenTelemetrySdk openTelemetrySdk;
        synchronized (mutex) {
            openTelemetrySdk = this.openTelemetrySdk;
        }
        return openTelemetrySdk == null ? CompletableResultCode.ofSuccess() : openTelemetrySdk.getSdkTracerProvider().forceFlush();
    }

    @Override
    public void close() {
        synchronized (mutex) {
            if (openTelemetrySdk != null) {
                openTelemetrySdk.getSdkTracerProvider().close();
                openTelemetrySdk = null;
            }
        }
    }

    private OpenTelemetrySdk createOpenTelemetrySdk() {
        String endpoint = OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalStateException(
                OTEL_TRACES_ENABLED_SYSTEM_PROPERTY + "=true requires telemetry.export.endpoint to be configured"
            );
        }

        TimeValue interval = OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL.get(settings);
        double sampleRate = OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.get(settings);
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
        String authHeader = OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings);
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

        // ParentBased honors a sampled upstream traceparent regardless of sampleRate; only locally-started
        // traces are subject to the ratio.
        Sampler sampler = Sampler.parentBased(Sampler.traceIdRatioBased(sampleRate));

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
            .setResource(OtelSdkResource.get(settings))
            .setSampler(sampler)
            .addSpanProcessor(processor)
            .build();

        return OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
    }
}
