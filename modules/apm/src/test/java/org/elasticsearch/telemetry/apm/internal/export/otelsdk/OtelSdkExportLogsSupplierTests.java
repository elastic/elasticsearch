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

import java.nio.file.Path;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ThreadLeakFilters(filters = { OkHttpThreadsFilter.class })
public class OtelSdkExportLogsSupplierTests extends ESTestCase {

    public void testInstallWhenDisabledIsNoop() {
        OtelSdkExportLogsSupplier supplier = new OtelSdkExportLogsSupplier(Settings.EMPTY, createTempDir());
        supplier.install();
        assertFalse("disabled supplier must not install an SDK", supplier.isInstalled());
        supplier.close();
    }

    public void testLogsEnabledWithoutEndpointIsInvalidSettings() {
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED.getKey(), true).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED, OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT)
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.validate(settings, true));
        assertThat(e.getMessage(), containsString("telemetry.logs.endpoint"));
    }

    public void testLogsEnabledWithEmptyEndpointIsInvalidSettings() {
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.getKey(), "")
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED, OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT)
        );
        expectThrows(IllegalArgumentException.class, () -> clusterSettings.validate(settings, true));
    }

    public void testInstallTwiceIsIdempotent() {
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .build();
        OtelSdkExportLogsSupplier supplier = new OtelSdkExportLogsSupplier(settings, createTempDir());
        try {
            supplier.install();
            supplier.install(); // second call must be a no-op
        } finally {
            supplier.close();
        }
    }

    public void testMaxQueueSizeDefault() {
        assertThat(OtelSdkSettings.TELEMETRY_LOGS_MAX_QUEUE_SIZE.get(Settings.EMPTY), equalTo(10_000));
    }

    public void testMaxQueueSizeBelowMinimumIsRejected() {
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_LOGS_MAX_QUEUE_SIZE.getKey(), 0).build();
        expectThrows(IllegalArgumentException.class, () -> OtelSdkSettings.TELEMETRY_LOGS_MAX_QUEUE_SIZE.get(settings));
    }

    public void testInstallWithCustomTimeoutRetryAndQueueSizeDoesNotThrow() {
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), "200ms")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_CONNECT_TIMEOUT.getKey(), "1ms")
            .put(OtelSdkSettings.TELEMETRY_LOGS_MAX_QUEUE_SIZE.getKey(), 42)
            .build();
        OtelSdkExportLogsSupplier supplier = new OtelSdkExportLogsSupplier(settings, createTempDir());
        try {
            supplier.install(); // must not throw despite the custom timeout/retry/queue-size settings
        } finally {
            supplier.close();
        }
    }

    public void testSslCertWithoutKeyIsRejected() {
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE.getKey(), getDataPath("tls/cert1.crt").toString())
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE, OtelSdkSettings.TELEMETRY_LOGS_SSL_KEY)
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.validate(settings, true));
        assertThat(e.getMessage(), containsString("telemetry.logs.ssl.certificate"));
        assertThat(e.getMessage(), containsString("telemetry.logs.ssl.key"));
    }

    public void testSslKeyWithoutCertIsRejected() {
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_SSL_KEY.getKey(), getDataPath("tls/cert1.key").toString())
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE, OtelSdkSettings.TELEMETRY_LOGS_SSL_KEY)
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.validate(settings, true));
        assertThat(e.getMessage(), containsString("telemetry.logs.ssl.certificate"));
        assertThat(e.getMessage(), containsString("telemetry.logs.ssl.key"));
    }

    public void testInstallWithCaDoesNotThrow() {
        Path caPath = getDataPath("tls/ca.crt");
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .putList(OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE_AUTHORITIES.getKey(), caPath.toString())
            .build();
        OtelSdkExportLogsSupplier supplier = new OtelSdkExportLogsSupplier(settings, caPath.getParent());
        try {
            supplier.install();
        } finally {
            supplier.close();
        }
    }

    public void testInstallWithFullMtlsDoesNotThrow() {
        Path caPath = getDataPath("tls/ca.crt");
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .putList(OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE_AUTHORITIES.getKey(), caPath.toString())
            .put(OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE.getKey(), getDataPath("tls/cert1.crt").toString())
            .put(OtelSdkSettings.TELEMETRY_LOGS_SSL_KEY.getKey(), getDataPath("tls/cert1.key").toString())
            .build();
        OtelSdkExportLogsSupplier supplier = new OtelSdkExportLogsSupplier(settings, caPath.getParent());
        try {
            supplier.install();
        } finally {
            supplier.close();
        }
    }

    public void testCloseWithoutInstallDoesNotThrow() {
        new OtelSdkExportLogsSupplier(Settings.EMPTY, createTempDir()).close();
    }

    public void testDoubleCloseAfterInstallDoesNotThrow() {
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .build();
        OtelSdkExportLogsSupplier supplier = new OtelSdkExportLogsSupplier(settings, createTempDir());
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
