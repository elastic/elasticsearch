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
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InternalTelemetryVersion;
import io.opentelemetry.sdk.common.export.RetryPolicy;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.export.MeterSupplier;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;

/**
 * A {@link MeterSupplier} that supplies meters that export telemetry using the OTel SDK.
 *
 * @see OtelSdkSettings
 * @see org.elasticsearch.telemetry.apm.internal.export.agent.AgentExportMeterSupplier
 */
public class OtelSdkExportMeterSupplier implements MeterSupplier {
    private final Settings settings;
    private final Path diskBufferPath;
    private volatile OTelMetricsResources resources;
    private final Object mutex = new Object();

    public OtelSdkExportMeterSupplier(Settings settings, Path diskBufferPath) {
        this.settings = settings;
        this.diskBufferPath = diskBufferPath;
    }

    /** For testing: pre-initializes resources so tests can inject readable providers. */
    OtelSdkExportMeterSupplier(Settings settings, Path diskBufferPath, OTelMetricsResources testResources) {
        this.settings = settings;
        this.diskBufferPath = diskBufferPath;
        this.resources = testResources;
    }

    @Override
    public Meter get() {
        synchronized (mutex) {
            if (resources == null) {
                resources = createMeteringResources();
            }
            return resources.meterProvider().get("elasticsearch");
        }
    }

    private OTelMetricsResources createMeteringResources() {
        var meterProviderRef = new AtomicReference<>(MeterProvider.noop());

        var systemProvider = buildSystemMeterProvider(meterProviderRef::get);
        meterProviderRef.set(systemProvider);
        var otelSdk = OpenTelemetrySdk.builder().setMeterProvider(systemProvider).build();

        // RuntimeTelemetry uses JMX (Java 8+) and JFR (Java 17+) to collect JVM metrics. See https://ela.st/otel-runtime-telemetry
        var runtimeTelemetry = OtelSdkSettings.TELEMETRY_OTEL_METRICS_ENABLED.get(settings) ? RuntimeTelemetry.create(otelSdk) : null;
        return new OTelMetricsResources(systemProvider, runtimeTelemetry);
    }

    private SdkMeterProvider buildSystemMeterProvider(Supplier<MeterProvider> meterProviderSupplier) {
        var exporter = wrapWithBuffering(createOTLPExporter(meterProviderSupplier), diskBufferPath, meterProviderSupplier);
        return sdkMeterProvider(buildMetricReader(exporter));
    }

    private MetricExporter wrapWithBuffering(
        OtlpHttpMetricExporter delegate,
        Path bufferPath,
        Supplier<MeterProvider> meterProviderSupplier
    ) {
        if (OtelSdkSettings.TELEMETRY_OTEL_METRICS_DISK_BUFFER_SIZE.get(settings).getBytes() == 0) {
            return delegate;
        }
        return new BufferingMetricExporter(delegate, settings, bufferPath, meterProviderSupplier);
    }

    private PeriodicMetricReader buildMetricReader(MetricExporter exporter) {
        return PeriodicMetricReader.builder(exporter)
            .setInterval(OtelSdkSettings.TELEMETRY_OTEL_METRICS_INTERVAL.get(settings).toDuration())
            .build();
    }

    private SdkMeterProvider sdkMeterProvider(PeriodicMetricReader reader) {
        return SdkMeterProvider.builder().setResource(OtelSdkResource.get(settings)).registerMetricReader(reader).build();
    }

    private OtlpHttpMetricExporter createOTLPExporter(Supplier<MeterProvider> meterProviderSupplier) {
        String endpoint = OtelSdkSettings.TELEMETRY_OTEL_METRICS_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalStateException(
                OTEL_METRICS_ENABLED_SYSTEM_PROPERTY + "=true requires telemetry.otel.metrics.endpoint to be configured"
            );
        }
        OtlpHttpMetricExporterBuilder builder = OtlpHttpMetricExporter.builder()
            .setEndpoint(endpoint)
            .setMeterProvider(meterProviderSupplier)
            .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred())
            .setInternalTelemetryVersion(InternalTelemetryVersion.LATEST)
            .setTimeout(OtelSdkSettings.TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.get(settings).toDuration())
            .setConnectTimeout(OtelSdkSettings.TELEMETRY_OTEL_OTLP_CONNECT_TIMEOUT.get(settings).toDuration())
            .setRetryPolicy(buildRetryPolicy());
        String authHeader = buildOtlpAuthorizationHeader(settings);
        if (authHeader != null) {
            builder.addHeader("Authorization", authHeader);
        }
        return builder.build();
    }

    private RetryPolicy buildRetryPolicy() {
        int maxAttempts = OtelSdkSettings.TELEMETRY_OTEL_OTLP_RETRY_MAX_ATTEMPTS.get(settings);
        if (maxAttempts <= 1) {
            return null;
        }
        return RetryPolicy.builder()
            .setMaxAttempts(maxAttempts)
            .setInitialBackoff(OtelSdkSettings.TELEMETRY_OTEL_OTLP_RETRY_INITIAL_BACKOFF.get(settings).toDuration())
            .setBackoffMultiplier(OtelSdkSettings.TELEMETRY_OTEL_OTLP_RETRY_BACKOFF_MULTIPLIER.get(settings))
            .build();
    }

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

    /** Flushes the meter provider. Callers must join the result with an appropriate timeout. */
    @Override
    public CompletableResultCode attemptFlushMetrics() {
        OTelMetricsResources resources;
        synchronized (mutex) {
            resources = this.resources;
        }
        return resources == null ? CompletableResultCode.ofSuccess() : resources.meterProvider.forceFlush();
    }

    /**
     * {@code BatchSpanProcessor} registers its queue-metric instruments exactly once on the first span,
     * so the provider it receives here is permanent. Initializing eagerly ensures that happens against
     * the real SDK provider even if metrics have not been explicitly enabled yet.
     */
    @Override
    public MeterProvider getMeterProvider() {
        synchronized (mutex) {
            if (resources == null) {
                resources = createMeteringResources();
            }
            return resources.meterProvider();
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

    record OTelMetricsResources(SdkMeterProvider meterProvider, RuntimeTelemetry runtimeTelemetry) implements AutoCloseable {

        OTelMetricsResources {
            Objects.requireNonNull(meterProvider, "meterProvider");
        }

        @Override
        public void close() {
            if (runtimeTelemetry != null) {
                runtimeTelemetry.close();
            }
            meterProvider.close();
        }
    }
}
