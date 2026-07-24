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
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InternalTelemetryVersion;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@ThreadLeakFilters(filters = { OkHttpThreadsFilter.class })
public class OtelSdkExportMeterSupplierTests extends ESTestCase {

    public void testGetWithoutEndpointThrows() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new OtelSdkExportMeterSupplier(Settings.EMPTY, null).get()
        );
        assertThat(e.getMessage(), containsString(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY));
        assertThat(e.getMessage(), containsString("telemetry.export.endpoint"));
    }

    public void testGetWithEmptyEndpointThrows() {
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), "").build();
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
        String bogusUrl = "http://127.0.0.1:9";
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), bogusUrl).build();
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
        String bogusUrl = "http://127.0.0.1:9";
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), bogusUrl).build();
        OtelSdkExportMeterSupplier supplier = new OtelSdkExportMeterSupplier(settings, createTempDir());
        assertThat(supplier.getMeterProvider(), org.hamcrest.Matchers.instanceOf(io.opentelemetry.sdk.metrics.SdkMeterProvider.class));
        supplier.close();
    }

    public void testCloseWithoutGetDoesNotThrow() {
        new OtelSdkExportMeterSupplier(Settings.EMPTY, null).close();
    }

    public void testDoubleCloseAfterGetDoesNotThrow() {
        String bogusUrl = "http://127.0.0.1:9";
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), bogusUrl).build();
        OtelSdkExportMeterSupplier supplier = new OtelSdkExportMeterSupplier(settings, createTempDir());
        supplier.get();
        supplier.close();
        supplier.close();
    }

    public void testSpanProcessorSelfMonitoringMetricsFlowIntoHealthProvider() {
        InMemoryMetricReader inMemoryReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder().registerMetricReader(inMemoryReader).build();
        var resources = new OtelSdkExportMeterSupplier.OTelMetricsResources(meterProvider, null, null);
        OtelSdkExportMeterSupplier meterSupplier = new OtelSdkExportMeterSupplier(Settings.EMPTY, null, resources);

        BatchSpanProcessor processor = BatchSpanProcessor.builder(InMemorySpanExporter.create())
            .setMeterProvider(meterSupplier::getMeterProvider)
            .setInternalTelemetryVersion(InternalTelemetryVersion.LATEST)
            .build();
        try (var tracerProvider = SdkTracerProvider.builder().setSampler(Sampler.alwaysOn()).addSpanProcessor(processor).build()) {
            tracerProvider.get("test").spanBuilder("test").startSpan().end();
            var metricNames = inMemoryReader.collectAllMetrics().stream().map(MetricData::getName).toList();
            assertThat(
                "expected otel.sdk.processor.span.queue.capacity in OTel meter provider",
                metricNames,
                hasItem("otel.sdk.processor.span.queue.capacity")
            );
        }
        meterSupplier.close();
    }

    public void testDisabledMetricViewsDropMatchingInstrumentsAndSkipCallbacks() {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        Settings settings = Settings.builder().putList(OtelSdkSettings.TELEMETRY_METRICS_DISABLED.getKey(), "es.test.dropped.*").build();
        var builder = SdkMeterProvider.builder().registerMetricReader(reader);
        OtelSdkExportMeterSupplier.registerDisabledMetricViews(builder, settings);

        AtomicBoolean droppedCallbackRan = new AtomicBoolean(false);
        AtomicBoolean keptCallbackRan = new AtomicBoolean(false);
        try (SdkMeterProvider provider = builder.build()) {
            Meter meter = provider.get("elasticsearch");
            meter.gaugeBuilder("es.test.dropped.value").buildWithCallback(m -> {
                droppedCallbackRan.set(true);
                m.record(1.0);
            });
            meter.gaugeBuilder("es.test.kept.value").buildWithCallback(m -> {
                keptCallbackRan.set(true);
                m.record(2.0);
            });

            var names = reader.collectAllMetrics().stream().map(MetricData::getName).toList();
            assertThat(names, hasItem("es.test.kept.value"));
            assertThat(names, not(hasItem("es.test.dropped.value")));
            assertTrue("kept instrument callback should run", keptCallbackRan.get());
            assertFalse("dropped instrument callback must be skipped", droppedCallbackRan.get());
        }
    }

    public void testExportIntervalGaugeReportsConfiguredInterval() {
        InMemoryMetricReader inMemoryReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder().registerMetricReader(inMemoryReader).build();
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL.getKey(), "90s").build();
        OtelSdkExportMeterSupplier supplier = new OtelSdkExportMeterSupplier(settings, null);

        try (var gauge = supplier.registerReaderMetrics(meterProvider)) {
            MetricData metric = inMemoryReader.collectAllMetrics()
                .stream()
                .filter(m -> m.getName().equals("es.apm.metrics.reader.export_interval"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("es.apm.metrics.reader.export_interval gauge was not emitted"));
            assertEquals("s", metric.getUnit());
            double value = metric.getDoubleGaugeData().getPoints().iterator().next().getValue();
            assertEquals(90.0, value, 0.0);
        }
        meterProvider.close();
    }

    /** attemptFlushMetrics() after close() must return a successful no-op result. */
    public void testAttemptFlushMetricsAfterCloseIsNoop() {
        String bogusUrl = "http://127.0.0.1:9";
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), bogusUrl).build();
        OtelSdkExportMeterSupplier supplier = new OtelSdkExportMeterSupplier(settings, createTempDir());
        supplier.get();
        supplier.close();
        CompletableResultCode result = supplier.attemptFlushMetrics();
        result.join(5, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(result.isSuccess());
    }
}
