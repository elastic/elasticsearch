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
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.instrumentation.runtimetelemetry.RuntimeTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InternalTelemetryVersion;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.TrustEverythingConfig;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.export.MeterSupplier;

import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;

/**
 * A {@link MeterSupplier} that supplies meters that export telemetry using the OTel SDK.
 *
 * @see OtelSdkSettings
 * @see org.elasticsearch.telemetry.apm.internal.export.agent.AgentExportMeterSupplier
 */
public class OtelSdkExportMeterSupplier implements MeterSupplier {

    // Internal JVM system property that enables OTel RuntimeTelemetry JVM metrics.
    private static final String OTEL_JVM_METRICS_ENABLED_SYSTEM_PROPERTY = "telemetry.metrics.otel_jvm.enabled";

    // Per-instrument-stream cardinality limit
    private static final int METRIC_CARDINALITY_LIMIT = 1000;

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
        var readerExportInterval = registerReaderMetrics(systemProvider);
        var otelSdk = OpenTelemetrySdk.builder().setMeterProvider(systemProvider).build();

        // RuntimeTelemetry uses JMX (Java 8+) and JFR (Java 17+) to collect JVM metrics. See https://ela.st/otel-runtime-telemetry
        var runtimeTelemetry = OTelJvmMetricsEnabled() ? RuntimeTelemetry.create(otelSdk) : null;
        return new OTelMetricsResources(systemProvider, runtimeTelemetry, readerExportInterval);
    }

    /**
     * Registers a gauge reporting the configured metric reader export interval. Paired with the SDK's
     * {@code otel.sdk.metric_reader.collection.duration}, it lets alerting compute collection overhead as a
     * fraction of the cycle without hardcoding the interval, so the two stay in sync when the setting changes.
     */
    ObservableDoubleGauge registerReaderMetrics(SdkMeterProvider provider) {
        double intervalSeconds = OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL.get(settings).millis() / 1000.0;
        return provider.get("elasticsearch")
            .gaugeBuilder("es.apm.metrics.reader.export_interval")
            .setDescription("Configured metric reader export interval, for computing collection overhead")
            .setUnit("s")
            .buildWithCallback(measurement -> measurement.record(intervalSeconds));
    }

    private static boolean OTelJvmMetricsEnabled() {
        return Booleans.parseBoolean(System.getProperty(OTEL_JVM_METRICS_ENABLED_SYSTEM_PROPERTY, "false"));
    }

    private SdkMeterProvider buildSystemMeterProvider(Supplier<MeterProvider> meterProviderSupplier) {
        var exporter = wrapWithBuffering(createOTLPExporter(meterProviderSupplier), diskBufferPath, meterProviderSupplier);
        return sdkMeterProvider(buildMetricReader(exporter));
    }

    private MetricExporter wrapWithBuffering(
        OtlpGrpcMetricExporter delegate,
        Path bufferPath,
        Supplier<MeterProvider> meterProviderSupplier
    ) {
        if (OtelSdkSettings.TELEMETRY_METRICS_BUFFER_DISK_SIZE.get(settings).getBytes() == 0) {
            return delegate;
        }
        return new BufferingMetricExporter(delegate, settings, bufferPath, meterProviderSupplier);
    }

    private PeriodicMetricReader buildMetricReader(MetricExporter exporter) {
        return PeriodicMetricReader.builder(exporter)
            .setInterval(OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL.get(settings).toDuration())
            .build();
    }

    private SdkMeterProvider sdkMeterProvider(PeriodicMetricReader reader) {
        return SdkMeterProvider.builder()
            .setResource(OtelSdkResource.get(settings))
            .registerMetricReader(reader, instrumentType -> METRIC_CARDINALITY_LIMIT)
            .build();
    }

    private OtlpGrpcMetricExporter createOTLPExporter(Supplier<MeterProvider> meterProviderSupplier) {
        String endpoint = OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalStateException(
                OTEL_METRICS_ENABLED_SYSTEM_PROPERTY + "=true requires telemetry.export.endpoint to be configured"
            );
        }
        OtlpGrpcMetricExporterBuilder builder = OtlpGrpcMetricExporter.builder()
            .setEndpoint(endpoint)
            .setMeterProvider(meterProviderSupplier)
            .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred())
            .setInternalTelemetryVersion(InternalTelemetryVersion.LATEST)
            .setTimeout(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.get(settings).toDuration())
            .setConnectTimeout(OtelSdkSettings.TELEMETRY_EXPORT_CONNECT_TIMEOUT.get(settings).toDuration())
            .setRetryPolicy(OtelSdkSettings.OTLP_RETRY_POLICY);
        String authHeader = buildOtlpAuthorizationHeader(settings);
        if (authHeader != null) {
            builder.addHeader("Authorization", authHeader);
        }
        configureTls(settings, builder::setSslContext);
        return builder.build();
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

    static void configureTls(Settings settings, BiConsumer<SSLContext, X509TrustManager> sslContextSetter) {
        if (OtelSdkSettings.TELEMETRY_EXPORT_VERIFY_SERVER_CERT.get(settings)) {
            return;
        }
        X509ExtendedTrustManager trustAll = TrustEverythingConfig.TRUST_EVERYTHING.createTrustManager();
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, new TrustManager[] { trustAll }, null);
            sslContextSetter.accept(context, trustAll);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
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

    record OTelMetricsResources(
        SdkMeterProvider meterProvider,
        RuntimeTelemetry runtimeTelemetry,
        ObservableDoubleGauge readerExportInterval
    ) implements AutoCloseable {

        OTelMetricsResources {
            Objects.requireNonNull(meterProvider, "meterProvider");
        }

        @Override
        public void close() {
            if (readerExportInterval != null) {
                readerExportInterval.close();
            }
            if (runtimeTelemetry != null) {
                runtimeTelemetry.close();
            }
            meterProvider.close();
        }
    }
}
