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
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_TRACES_ENABLED_SYSTEM_PROPERTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;

@ThreadLeakFilters(filters = { OkHttpThreadsFilter.class })
public class OtelSdkExportTracerSupplierTests extends ESTestCase {

    public void testConstructorWithoutEndpointThrows() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new OtelSdkExportTracerSupplier(Settings.EMPTY, MeterProvider::noop)
        );
        assertThat(e.getMessage(), containsString(OTEL_TRACES_ENABLED_SYSTEM_PROPERTY));
        assertThat(e.getMessage(), containsString("telemetry.export.endpoint"));
    }

    public void testConstructorWithEmptyEndpointThrows() {
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), "").build();
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new OtelSdkExportTracerSupplier(settings, MeterProvider::noop)
        );
        assertThat(e.getMessage(), containsString(OTEL_TRACES_ENABLED_SYSTEM_PROPERTY));
        assertThat(e.getMessage(), containsString("telemetry.export.endpoint"));
    }

    public void testConstructorWithNoopMeterProviderDoesNotThrow() {
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), "200ms")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL.getKey(), "300ms")
            .build();
        try (var supplier = new OtelSdkExportTracerSupplier(settings, MeterProvider::noop)) {
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
        try (var supplier = new OtelSdkExportTracerSupplier(settings, () -> meterProvider)) {
            // Start and end a span so BatchSpanProcessor registers its queue metrics.
            var span = supplier.get().getTracer("test").spanBuilder("test").startSpan();
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
}
