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
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporterBuilder;
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
 * {@link TraceSupplier} that exports spans via OTLP HTTP using its own {@link SdkTracerProvider},
 * used when {@code telemetry.otel.traces.enabled=true} is set as a JVM system property.
 */
public class OtelSdkExportTracerSupplier implements TraceSupplier {

    private final SdkTracerProvider tracerProvider;
    private final OpenTelemetrySdk openTelemetrySdk;

    public OtelSdkExportTracerSupplier(Settings settings, Supplier<MeterProvider> meterProvider) {
        String endpoint = OtelSdkSettings.TELEMETRY_OTEL_TRACES_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalStateException(
                OTEL_TRACES_ENABLED_SYSTEM_PROPERTY + "=true requires telemetry.otel.traces.endpoint to be configured"
            );
        }

        TimeValue interval = OtelSdkSettings.TELEMETRY_OTEL_TRACES_INTERVAL.get(settings);
        double sampleRate = OtelSdkSettings.TELEMETRY_OTEL_TRACES_SAMPLE_RATE.get(settings);
        int maxQueueSize = OtelSdkSettings.TELEMETRY_OTEL_TRACES_BATCH_MAX_QUEUE_SIZE.get(settings);
        int maxExportBatchSize = OtelSdkSettings.TELEMETRY_OTEL_TRACES_BATCH_MAX_EXPORT_BATCH_SIZE.get(settings);
        TimeValue exportTimeout = OtelSdkSettings.TELEMETRY_OTEL_TRACES_BATCH_EXPORT_TIMEOUT.get(settings);

        // InternalTelemetryVersion is @Internal but is the only way to opt into stable SemConv names in 1.62.0.
        OtlpHttpSpanExporterBuilder builder = OtlpHttpSpanExporter.builder()
            .setEndpoint(endpoint)
            .setMeterProvider(meterProvider)
            .setInternalTelemetryVersion(InternalTelemetryVersion.LATEST);
        String authHeader = OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings);
        if (authHeader != null) {
            builder.addHeader("Authorization", authHeader);
        }
        OtlpHttpSpanExporter exporter = builder.build();

        BatchSpanProcessor processor = BatchSpanProcessor.builder(exporter)
            .setMeterProvider(meterProvider)
            .setInternalTelemetryVersion(InternalTelemetryVersion.LATEST)
            .setScheduleDelay(interval.millis(), TimeUnit.MILLISECONDS)
            .setMaxQueueSize(maxQueueSize)
            .setMaxExportBatchSize(maxExportBatchSize)
            .setExporterTimeout(exportTimeout.millis(), TimeUnit.MILLISECONDS)
            .build();

        // ParentBased honors a sampled upstream traceparent regardless of sampleRate; only locally-started
        // traces are subject to the ratio.
        Sampler sampler = Sampler.parentBased(Sampler.traceIdRatioBased(sampleRate));

        this.tracerProvider = SdkTracerProvider.builder()
            .setResource(OtelSdkResource.get(settings))
            .setSampler(sampler)
            .addSpanProcessor(processor)
            .build();

        this.openTelemetrySdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
    }

    @Override
    public OpenTelemetry get() {
        return openTelemetrySdk;
    }

    @Override
    public CompletableResultCode attemptFlushTraces() {
        return tracerProvider.forceFlush();
    }

    @Override
    public void close() {
        tracerProvider.close();
    }
}
