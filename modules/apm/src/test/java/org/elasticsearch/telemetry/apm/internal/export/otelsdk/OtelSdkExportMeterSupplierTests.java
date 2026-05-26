/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class OtelSdkExportMeterSupplierTests extends ESTestCase {

    public void testGetWithoutEndpointThrows() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> new OtelSdkExportMeterSupplier(Settings.EMPTY).get());
        assertThat(e.getMessage(), containsString(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY));
        assertThat(e.getMessage(), containsString("telemetry.otel.metrics.endpoint"));
    }

    public void testGetWithEmptyEndpointThrows() {
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_OTEL_METRICS_ENDPOINT.getKey(), "").build();
        expectThrows(IllegalStateException.class, () -> new OtelSdkExportMeterSupplier(settings).get());
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

    public void testCloseWithoutGetDoesNotThrow() {
        new OtelSdkExportMeterSupplier(Settings.EMPTY).close();
    }

    public void testDoubleCloseAfterGetDoesNotThrow() {
        String bogusUrl = "http://127.0.0.1:9/v1/metrics";
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_OTEL_METRICS_ENDPOINT.getKey(), bogusUrl).build();
        OtelSdkExportMeterSupplier supplier = new OtelSdkExportMeterSupplier(settings);
        supplier.get();
        supplier.close();
        supplier.close();
    }

    /** attemptFlushMetrics() after close() must return a successful no-op result. */
    public void testAttemptFlushMetricsAfterCloseIsNoop() {
        OtelSdkExportMeterSupplier supplier = new OtelSdkExportMeterSupplier(testResources(InMemoryMetricExporter.create()));
        supplier.close();
        CompletableResultCode result = supplier.attemptFlushMetrics();
        result.join(5, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(result.isSuccess());
    }

    /** attemptFlushMetrics() with initialized resources triggers an export of buffered metric data. */
    public void testAttemptFlushMetricsTriggersExport() {
        InMemoryMetricExporter exporter = InMemoryMetricExporter.create();
        OtelSdkExportMeterSupplier supplier = new OtelSdkExportMeterSupplier(testResources(exporter));

        supplier.get().counterBuilder("test.counter").build().add(1);
        supplier.attemptFlushMetrics().join(5, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(
            "expected at least one metric export after attemptFlushMetrics",
            exporter.getFinishedMetricItems().size(),
            greaterThan(0)
        );
        supplier.close();
    }

    /**
     * Verifies that health metrics recorded during the system export are captured by the health flush.
     * This requires the system provider to flush before the health provider in each cycle.
     */
    public void testAttemptFlushMetricsCapturesHealthMetricsRecordedDuringSystemExport() throws Exception {
        InMemoryMetricExporter healthExporter = InMemoryMetricExporter.create();
        var healthProvider = SdkMeterProvider.builder().registerMetricReader(PeriodicMetricReader.builder(healthExporter).build()).build();
        var healthCounter = healthProvider.meterBuilder("test").build().counterBuilder("health.signal").build();

        MetricExporter systemExporter = new MetricExporter() {
            @Override
            public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
                return AggregationTemporality.CUMULATIVE;
            }

            @Override
            public CompletableResultCode export(Collection<MetricData> metrics) {
                healthCounter.add(1);
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode flush() {
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode shutdown() {
                return CompletableResultCode.ofSuccess();
            }
        };

        var systemProvider = SdkMeterProvider.builder().registerMetricReader(PeriodicMetricReader.builder(systemExporter).build()).build();
        systemProvider.meterBuilder("test").build().counterBuilder("system.metric").build().add(1);

        var supplier = new OtelSdkExportMeterSupplier(
            new OtelSdkExportMeterSupplier.OTelMetricsResources(systemProvider, healthProvider, null)
        );
        supplier.attemptFlushMetrics().join(5, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(
            "health data recorded during system export must be captured by the health flush",
            healthExporter.getFinishedMetricItems().stream().map(MetricData::getName).anyMatch("health.signal"::equals),
            is(true)
        );
        supplier.close();
    }

    private static OtelSdkExportMeterSupplier.OTelMetricsResources testResources(InMemoryMetricExporter exporter) {
        var provider = SdkMeterProvider.builder().registerMetricReader(PeriodicMetricReader.builder(exporter).build()).build();
        return new OtelSdkExportMeterSupplier.OTelMetricsResources(provider, provider, null);
    }
}
