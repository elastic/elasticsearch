/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ThreadLeakFilters(filters = { OkHttpThreadsFilter.class })
public class OtelSdkExportLogsSupplierTests extends ESTestCase {

    public void testInstallWhenDisabledIsNoop() {
        OtelSdkExportLogsSupplier supplier = new OtelSdkExportLogsSupplier(Settings.EMPTY);
        supplier.install();
        assertFalse("disabled supplier must not install an SDK", supplier.isInstalled());
        supplier.close();
    }

    public void testLogsEnabledWithoutEndpointIsInvalidSettings() {
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENABLED.getKey(), true).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENABLED, OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENDPOINT)
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.validate(settings, true));
        assertThat(e.getMessage(), containsString("telemetry.otel.logs.endpoint"));
    }

    public void testLogsEnabledWithEmptyEndpointIsInvalidSettings() {
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENABLED.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENDPOINT.getKey(), "")
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENABLED, OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENDPOINT)
        );
        expectThrows(IllegalArgumentException.class, () -> clusterSettings.validate(settings, true));
    }

    public void testInstallTwiceIsIdempotent() {
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENABLED.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENDPOINT.getKey(), "http://127.0.0.1:9/v1/logs")
            .build();
        OtelSdkExportLogsSupplier supplier = new OtelSdkExportLogsSupplier(settings);
        try {
            supplier.install();
            supplier.install(); // second call must be a no-op
        } finally {
            supplier.close();
        }
    }

    public void testCloseWithoutInstallDoesNotThrow() {
        new OtelSdkExportLogsSupplier(Settings.EMPTY).close();
    }

    public void testDoubleCloseAfterInstallDoesNotThrow() {
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENABLED.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENDPOINT.getKey(), "http://127.0.0.1:9/v1/logs")
            .build();
        OtelSdkExportLogsSupplier supplier = new OtelSdkExportLogsSupplier(settings);
        supplier.install();
        supplier.close();
        supplier.close();
    }

    /**
     * Direct SDK-side end-to-end check: emit a log record via the OTel logs API and assert it
     * reaches the configured exporter. Doesn't go through log4j; that path is exercised by the
     * upstream OpenTelemetryAppender library tests.
     */
    public void testSdkLogRecordReachesExporter() {
        InMemoryLogRecordExporter exporter = InMemoryLogRecordExporter.create();
        SdkLoggerProvider provider = SdkLoggerProvider.builder().addLogRecordProcessor(SimpleLogRecordProcessor.create(exporter)).build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setLoggerProvider(provider).build();
        try {
            sdk.getLogsBridge().get("test").logRecordBuilder().setBody("hello from audit").setSeverity(Severity.INFO).emit();

            assertThat(exporter.getFinishedLogRecordItems(), hasSize(1));
            assertThat(exporter.getFinishedLogRecordItems().getFirst().getBodyValue().asString(), equalTo("hello from audit"));
        } finally {
            provider.close();
        }
    }
}
