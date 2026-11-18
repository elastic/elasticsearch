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
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

@ThreadLeakFilters(filters = { OkHttpThreadsFilter.class })
public class OtelSdkExportTracerSupplierTests extends ESTestCase {

    public void testMissingEndpointReturnsNoopInsteadOfThrowing() {
        assertDegradesToNoop(Settings.EMPTY);
    }

    public void testEmptyEndpointReturnsNoopInsteadOfThrowing() {
        assertDegradesToNoop(Settings.builder().put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), "").build());
    }

    private void assertDegradesToNoop(Settings settings) {
        try (var supplier = supplier(settings, MeterProvider::noop)) {
            assertThat(supplier.get(), is(TracerProvider.noop()));
            assertThat(supplier.attemptFlushTraces().isSuccess(), is(true));
        }
    }

    public void testConstructorWithNoopMeterProviderDoesNotThrow() {
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), "200ms")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL.getKey(), "300ms")
            .build();
        try (var supplier = supplier(settings, MeterProvider::noop)) {
            assertNotNull(supplier.get());
        }
    }

    /**
     * Verifies that SDK self-monitoring metrics (otel.sdk.processor.span.*) are emitted into the
     * supplied MeterProvider when a real provider is wired in. Uses InMemoryMetricReader which
     * reads observable callbacks synchronously via collectAllMetrics().
     */
    public void testSdkSelfMonitoringMetricsEmittedIntoMeterProvider() {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), "200ms")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL.getKey(), "300ms")
            .put(OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.getKey(), 1.0)
            .build();
        try (var supplier = supplier(settings, () -> meterProvider)) {
            // Start and end a span so BatchSpanProcessor registers its queue metrics.
            var span = supplier.get().get("test").spanBuilder("test").startSpan();
            span.end();

            var metricNames = reader.collectAllMetrics().stream().map(MetricData::getName).toList();
            assertThat(
                "expected otel.sdk.processor.span.queue.capacity to appear in the health meter provider",
                metricNames,
                hasItem("otel.sdk.processor.span.queue.capacity")
            );
        }
        meterProvider.close();
    }

    public void testGetAfterSecureSettingsClosed() throws IOException {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("telemetry.secret_token", "tok");
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), "200ms")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL.getKey(), "300ms")
            .setSecureSettings(secureSettings)
            .build();
        try (var supplier = supplier(settings, MeterProvider::noop)) {
            secureSettings.close();
            supplier.get();
            assertThat(supplier.authHeader, equalTo("Bearer tok"));
        }
    }

    public void testGetReturnsTheSameInstanceOnceItIsCreated() {
        try (var supplier = supplier(exportingSettings(), MeterProvider::noop)) {
            final TracerProvider first = supplier.get();

            assertThat(first, not(is(TracerProvider.noop())));
            assertThat(supplier.get(), sameInstance(first));
        }
    }

    public void testGetAfterCloseDoesNotCreateANewSdk() {
        final var supplier = supplier(exportingSettings(), MeterProvider::noop);
        final TracerProvider sdk = supplier.get();
        supplier.close();

        for (int i = 0; i < 3; i++) {
            final TracerProvider afterClose = supplier.get();
            assertThat(afterClose, is(TracerProvider.noop()));
            assertThat(afterClose, not(sameInstance(sdk)));
            assertThat(afterClose.get("test").spanBuilder("name").startSpan().isRecording(), is(false));
        }
    }

    public void testCloseBeforeTheFirstGetPreventsTheSdkFromBeingCreated() {
        final var supplier = supplier(exportingSettings(), MeterProvider::noop);

        supplier.close();

        assertThat(supplier.get(), is(TracerProvider.noop()));
        assertThat(supplier.get().get("test").spanBuilder("name").startSpan().isRecording(), is(false));
    }

    public void testCloseIsIdempotent() {
        final var supplier = supplier(exportingSettings(), MeterProvider::noop);
        supplier.get();

        supplier.close();
        supplier.close();

        assertThat(supplier.get(), is(TracerProvider.noop()));
    }

    public void testSampledRootSpanCarriesLegacyOtTracestate() {
        try (var supplier = supplier(exportingSettings(), MeterProvider::noop)) {
            var span = supplier.get().get("test").spanBuilder("root").startSpan();
            assertThat(span.getSpanContext().getTraceState().get("ot"), equalTo("p:0"));
            span.end();
        }
    }

    private static OtelSdkExportTracerSupplier supplier(Settings settings, Supplier<MeterProvider> meterProvider) {
        return new OtelSdkExportTracerSupplier(settings, meterProvider);
    }

    /** Settings that make the supplier create an SDK on the first call to {@code get()}, and not to export anything anywhere. */
    private static Settings exportingSettings() {
        return Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), "200ms")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL.getKey(), "300ms")
            // every span is recorded, which is how a span of the SDK can be told from one of the no-op
            .put(OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.getKey(), 1.0)
            .build();
    }
}
