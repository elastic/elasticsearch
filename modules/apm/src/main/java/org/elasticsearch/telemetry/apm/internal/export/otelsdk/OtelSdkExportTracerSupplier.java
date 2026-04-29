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
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_TRACES_ENABLED_SYSTEM_PROPERTY;

/**
 * {@link TraceSupplier} that owns its own {@link SdkTracerProvider} and {@link OpenTelemetrySdk},
 * used when {@code telemetry.otel.traces.enabled=true} is set as a JVM system property.
 * <p>
 * Created once at startup and held by {@code APMTracer}. {@link #attemptFlushTraces()} forces
 * immediate export of buffered spans; {@link #close()} shuts the provider down.
 */
public class OtelSdkExportTracerSupplier implements TraceSupplier {

    private final SdkTracerProvider tracerProvider;
    private final OpenTelemetrySdk openTelemetrySdk;

    public OtelSdkExportTracerSupplier(Settings settings) {
        String endpoint = OtelSdkSettings.TELEMETRY_OTEL_TRACES_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalStateException(
                OTEL_TRACES_ENABLED_SYSTEM_PROPERTY + "=true requires telemetry.otel.traces.endpoint to be configured"
            );
        }

        TimeValue interval = OtelSdkSettings.TELEMETRY_OTEL_TRACES_INTERVAL.get(settings);

        OtlpHttpSpanExporterBuilder builder = OtlpHttpSpanExporter.builder().setEndpoint(endpoint);
        String authHeader = OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings);
        if (authHeader != null) {
            builder.addHeader("Authorization", authHeader);
        }
        OtlpHttpSpanExporter exporter = builder.build();

        BatchSpanProcessor processor = BatchSpanProcessor.builder(exporter)
            .setScheduleDelay(interval.millis(), TimeUnit.MILLISECONDS)
            .build();

        // Resource.getDefault() supplies the OTel SemConv telemetry.sdk.{name,version,language} keys.
        // The APM-intake-style equivalents (service.agent.{name,version}, service.language.name) are
        // layered on top so downstream consumers that read either naming scheme see both.
        Resource resource = Resource.getDefault()
            .merge(
                Resource.builder()
                    .put("service.name", "elasticsearch")
                    .put("service.version", Build.current().version())
                    .put("service.agent.name", "elasticsearch-otel-sdk")
                    .put("service.agent.version", Build.current().version())
                    .put("service.language.name", "java")
                    .build()
            );

        this.tracerProvider = SdkTracerProvider.builder().setResource(resource).addSpanProcessor(processor).build();

        this.openTelemetrySdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
    }

    @Override
    public OpenTelemetry get() {
        return openTelemetrySdk;
    }

    /** Forces an immediate export of any buffered spans. Blocks up to 10 seconds. */
    @Override
    public void attemptFlushTraces() {
        tracerProvider.forceFlush().join(10, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        tracerProvider.close();
    }
}
