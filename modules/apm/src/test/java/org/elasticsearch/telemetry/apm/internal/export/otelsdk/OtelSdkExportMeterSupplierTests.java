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
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.nullValue;

public class OtelSdkExportMeterSupplierTests extends ESTestCase {

    public void testGetWithoutEndpointThrows() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new OtelSdkExportMeterSupplier(Settings.EMPTY, null).get()
        );
        assertThat(e.getMessage(), containsString(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY));
        assertThat(e.getMessage(), containsString("telemetry.otel.metrics.endpoint"));
    }

    public void testGetWithEmptyEndpointThrows() {
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_OTEL_METRICS_ENDPOINT.getKey(), "").build();
        expectThrows(IllegalStateException.class, () -> new OtelSdkExportMeterSupplier(settings, null).get());
    }

    public void testBuildOtlpAuthorizationHeaderWithNeitherCredential() {
        assertThat(OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(Settings.EMPTY), nullValue());
    }

    public void testBuildOtlpAuthorizationHeaderPrefersApiKeyOverSecretToken() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("telemetry.api_key", "a2V5");
        secureSettings.setString("telemetry.secret_token", "tok");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        assertThat(OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings), equalTo("ApiKey a2V5"));
    }

    public void testBuildOtlpAuthorizationHeaderWithSecretTokenOnly() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("telemetry.secret_token", "sec");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        assertThat(OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings), equalTo("Bearer sec"));
    }

    public void testBuildOtlpAuthorizationHeaderWithApiKeyOnly() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("telemetry.api_key", "xyz");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        assertThat(OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings), equalTo("ApiKey xyz"));
    }

    public void testGetMeterProviderWithoutEndpointThrows() {
        expectThrows(IllegalStateException.class, () -> new OtelSdkExportMeterSupplier(Settings.EMPTY, null).getMeterProvider());
    }

    public void testGetMeterProviderAfterGetReturnsSdkProvider() {
        String bogusUrl = "http://127.0.0.1:9/v1/metrics";
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_OTEL_METRICS_ENDPOINT.getKey(), bogusUrl).build();
        OtelSdkExportMeterSupplier supplier = new OtelSdkExportMeterSupplier(settings, createTempDir());
        supplier.get();
        assertThat(supplier.getMeterProvider(), org.hamcrest.Matchers.instanceOf(io.opentelemetry.sdk.metrics.SdkMeterProvider.class));
        supplier.close();
    }

    /**
     * Verifies that getHealthMeterProvider() initializes resources even before get() is called, so that
     * BatchSpanProcessor instruments are registered against the real MeterProvider on the first span.
     */
    public void testGetHealthMeterProviderInitializesEagerlyBeforeGet() {
        String bogusUrl = "http://127.0.0.1:9/v1/metrics";
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_OTEL_METRICS_ENDPOINT.getKey(), bogusUrl).build();
        OtelSdkExportMeterSupplier supplier = new OtelSdkExportMeterSupplier(settings, createTempDir());
        assertThat(supplier.getMeterProvider(), org.hamcrest.Matchers.instanceOf(io.opentelemetry.sdk.metrics.SdkMeterProvider.class));
        supplier.close();
    }

    public void testCloseWithoutGetDoesNotThrow() {
        new OtelSdkExportMeterSupplier(Settings.EMPTY, null).close();
    }

    public void testDoubleCloseAfterGetDoesNotThrow() {
        String bogusUrl = "http://127.0.0.1:9/v1/metrics";
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_OTEL_METRICS_ENDPOINT.getKey(), bogusUrl).build();
        OtelSdkExportMeterSupplier supplier = new OtelSdkExportMeterSupplier(settings, createTempDir());
        supplier.get();
        supplier.close();
        supplier.close();
    }

    /**
     * Verifies end-to-end wiring: {@link OtelSdkExportTracerSupplier} emits
     * {@code otel.sdk.processor.span.*} self-monitoring metrics into the health
     * {@link MeterProvider} returned by {@link OtelSdkExportMeterSupplier#getMeterProvider()}.
     */
    public void testSpanProcessorSelfMonitoringMetricsFlowIntoHealthProvider() {
        InMemoryMetricReader inMemoryReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder().registerMetricReader(inMemoryReader).build();
        var resources = new OtelSdkExportMeterSupplier.OTelMetricsResources(meterProvider, null);
        OtelSdkExportMeterSupplier meterSupplier = new OtelSdkExportMeterSupplier(Settings.EMPTY, null, resources);
        Settings tracerSettings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_OTEL_TRACES_ENDPOINT.getKey(), "http://127.0.0.1:9/v1/traces")
            .put(OtelSdkSettings.TELEMETRY_OTEL_TRACES_INTERVAL.getKey(), "1ms")
            .put(OtelSdkSettings.TELEMETRY_OTEL_TRACES_SAMPLE_RATE.getKey(), 1.0)
            .build();
        try (var tracerSupplier = new OtelSdkExportTracerSupplier(tracerSettings, meterSupplier::getMeterProvider)) {
            var span = tracerSupplier.get().getTracer("test").spanBuilder("test").startSpan();
            span.end();
            var metricNames = inMemoryReader.collectAllMetrics().stream().map(MetricData::getName).toList();
            assertThat(
                "expected otel.sdk.processor.span.queue.capacity in OTel meter provider",
                metricNames,
                hasItem("otel.sdk.processor.span.queue.capacity")
            );
        }
        meterSupplier.close();
    }

    /** attemptFlushMetrics() after close() must return a successful no-op result. */
    public void testAttemptFlushMetricsAfterCloseIsNoop() {
        String bogusUrl = "http://127.0.0.1:9/v1/metrics";
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_OTEL_METRICS_ENDPOINT.getKey(), bogusUrl).build();
        OtelSdkExportMeterSupplier supplier = new OtelSdkExportMeterSupplier(settings, createTempDir());
        supplier.get();
        supplier.close();
        CompletableResultCode result = supplier.attemptFlushMetrics();
        result.join(5, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(result.isSuccess());
    }
}
