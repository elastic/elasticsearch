/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.instrumentation.runtimetelemetry.RuntimeTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.InternalTelemetryVersion;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.export.MeterSupplier;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;

/**
 * A {@link MeterSupplier} that supplies meters that export telemetry using the OTel SDK.
 *
 * @see OtelSdkSettings
 * @see org.elasticsearch.telemetry.apm.internal.export.agent.AgentExportMeterSupplier
 */
public class OtelSdkExportMeterSupplier implements MeterSupplier {
    private final Settings settings;
    private volatile OTelMetricsResources resources;
    private static final Object mutex = new Object();

    public OtelSdkExportMeterSupplier(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Meter get() {
        synchronized (mutex) {
            if (resources == null) {
                resources = createMeteringResources();
            }
            return resources.systemMeterProvider().get("elasticsearch");
        }
    }

    private OTelMetricsResources createMeteringResources() {
        TimeValue intervalTimeValue = OtelSdkSettings.TELEMETRY_OTEL_METRICS_INTERVAL.get(settings);

        // Reader to collect metrics about OTLPExporter
        var metricHealthReader = PeriodicMetricReader.builder(createOTLPExporter(MeterProvider.noop()))
            .setInterval(intervalTimeValue.toDuration())
            .build();
        var metricHealthProvider = sdkMeterProvider(metricHealthReader);

        var reader = PeriodicMetricReader.builder(createOTLPExporter(metricHealthProvider))
            .setInterval(intervalTimeValue.toDuration())
            .build();
        var systemMeterProvider = sdkMeterProvider(reader);
        var otelSdk = OpenTelemetrySdk.builder().setMeterProvider(systemMeterProvider).build();

        // RuntimeTelemetry uses JMX (Java 8+) and JFR (Java 17+) to collect JVM metrics. See https://ela.st/otel-runtime-telemetry
        var runtimeTelemetry = OtelSdkSettings.TELEMETRY_OTEL_METRICS_ENABLED.get(settings) ? RuntimeTelemetry.create(otelSdk) : null;
        return new OTelMetricsResources(systemMeterProvider, metricHealthProvider, runtimeTelemetry);
    }

    private static SdkMeterProvider sdkMeterProvider(PeriodicMetricReader reader) {
        return SdkMeterProvider.builder()
            .setResource(Resource.builder().put("service.name", "elasticsearch").build())
            .registerMetricReader(reader)
            .build();
    }

    private OtlpHttpMetricExporter createOTLPExporter(MeterProvider healthExportMeterProvider) {
        String endpoint = OtelSdkSettings.TELEMETRY_OTEL_METRICS_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalStateException(
                OTEL_METRICS_ENABLED_SYSTEM_PROPERTY + "=true requires telemetry.otel.metrics.endpoint to be configured"
            );
        }
        OtlpHttpMetricExporterBuilder builder = OtlpHttpMetricExporter.builder()
            .setEndpoint(endpoint)
            .setMeterProvider(() -> healthExportMeterProvider)
            .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred())
            .setInternalTelemetryVersion(InternalTelemetryVersion.LATEST);
        String authHeader = buildOtlpAuthorizationHeader(settings);
        if (authHeader != null) {
            builder.addHeader("Authorization", authHeader);
        }
        return builder.build();
    }

    /**
     * Authorization header for OTLP HTTP requests when API key or secret token is configured.
     */
    static String buildOtlpAuthorizationHeader(Settings settings) {
        try (SecureString apiKey = APMAgentSettings.TELEMETRY_API_KEY_SETTING.get(settings)) {
            if (apiKey.isEmpty() == false) {
                return "ApiKey " + apiKey;
            }
        }
        try (SecureString secretToken = APMAgentSettings.TELEMETRY_SECRET_TOKEN_SETTING.get(settings)) {
            if (secretToken.isEmpty() == false) {
                return "Bearer " + secretToken;
            }
        }
        return null;
    }

    @Override
    public void attemptFlushMetrics() {
        synchronized (mutex) {
            if (resources != null) {
                resources.systemMeterProvider.forceFlush().join(10, TimeUnit.SECONDS);
                resources.meterHealthMeterProvider.forceFlush().join(10, TimeUnit.SECONDS);
                // PeriodicMetricReader records collection.duration after
                // each collection, so a second cycle is required to collect and export it.
                resources.systemMeterProvider.forceFlush().join(10, TimeUnit.SECONDS);
                resources.meterHealthMeterProvider.forceFlush().join(10, TimeUnit.SECONDS);
            }
        }
    }

    @Override
    public void close() {
        synchronized (mutex) {
            if (resources != null) {
                resources.close();
                resources = null;
            }
        }
    }

    private record OTelMetricsResources(
        SdkMeterProvider systemMeterProvider,
        SdkMeterProvider meterHealthMeterProvider,
        RuntimeTelemetry runtimeTelemetry
    ) implements AutoCloseable {

        OTelMetricsResources {
            Objects.requireNonNull(systemMeterProvider, "systemMeterProvider");
            Objects.requireNonNull(meterHealthMeterProvider, "meterHealthMeterProvider");
        }

        @Override
        public void close() {
            if (runtimeTelemetry != null) {
                runtimeTelemetry.close();
            }
            systemMeterProvider.close();
            meterHealthMeterProvider.close();
        }
    }
}
