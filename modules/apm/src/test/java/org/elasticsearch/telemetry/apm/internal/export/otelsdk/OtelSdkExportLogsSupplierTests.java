/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.TelemetryLogResourceProvider;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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

    public void testLogDeliveryResourceContainsOnlyServiceNameAndType() {
        Resource resource = OtelSdkExportLogsSupplier.logDeliveryResource(TelemetryLogResourceProvider.DEFAULT_SERVICE_NAME);
        assertThat(resource.getAttributes().size(), equalTo(2));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.name")), equalTo("elasticsearch"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.type")), equalTo("elasticsearch"));
    }

    public void testLogDeliveryResourceUsesProvidedServiceName() {
        Resource resource = OtelSdkExportLogsSupplier.logDeliveryResource("elasticsearch-serverless");
        assertThat(resource.getAttributes().size(), equalTo(2));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.name")), equalTo("elasticsearch-serverless"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.type")), equalTo("elasticsearch"));
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

    /**
     * A destination that never accepts the connection is invisible to the gateway. The sender records
     * {@code otel.sdk.exporter.log.exported} with {@code error.type} once retries are exhausted.
     */
    public void testUnreachableDestinationReportsFailedExportMetric() {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_QUERYLOG_ENABLED.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), "200ms")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_CONNECT_TIMEOUT.getKey(), "1ms")
            .build();

        Runnable removeQueryLogger = addQueryLogger();
        OtelSdkExportLogsSupplier supplier = new OtelSdkExportLogsSupplier(
            settings,
            createTempDir(),
            List.of(),
            new TelemetryLogResourceProvider.Default(),
            () -> meterProvider
        );
        try {
            supplier.install();
            org.apache.logging.log4j.LogManager.getLogger(QueryLogging.QUERY_LOGGER_NAME).info("exporter metrics probe");
            assertTrue("log export flush did not finish", supplier.forceFlush().join(15, TimeUnit.SECONDS).isDone());

            MetricData exported = reader.collectAllMetrics()
                .stream()
                .filter(m -> m.getName().equals("otel.sdk.exporter.log.exported"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("otel.sdk.exporter.log.exported was not recorded"));
            LongPointData failed = exported.getLongSumData()
                .getPoints()
                .stream()
                .filter(p -> p.getAttributes().get(AttributeKey.stringKey("error.type")) != null)
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected error.type on a failed export: " + exported));
            assertThat(failed.getValue(), greaterThan(0L));
            assertThat(failed.getAttributes().get(AttributeKey.stringKey("server.address")), equalTo("127.0.0.1"));
            assertThat(failed.getAttributes().get(AttributeKey.longKey("server.port")), equalTo(9L));
        } finally {
            supplier.close();
            removeQueryLogger.run();
            meterProvider.close();
        }
    }

    /**
     * A cert reload must not register a second set of batch processor queue gauges. Those callbacks cannot be
     * unregistered and share the processor's static component name, so a second processor would be summed into
     * the same series and report twice the configured capacity.
     */
    public void testReloadKeepsQueueCapacityMetric() {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_QUERYLOG_ENABLED.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.getKey(), "http://127.0.0.1:9")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), "200ms")
            .put(OtelSdkSettings.TELEMETRY_EXPORT_CONNECT_TIMEOUT.getKey(), "1ms")
            .put(OtelSdkSettings.TELEMETRY_LOGS_MAX_QUEUE_SIZE.getKey(), 42)
            .build();
        Runnable removeQueryLogger = addQueryLogger();
        OtelSdkExportLogsSupplier supplier = new OtelSdkExportLogsSupplier(
            settings,
            createTempDir(),
            List.of(),
            new TelemetryLogResourceProvider.Default(),
            () -> meterProvider
        );
        try {
            supplier.install();
            var queryLogger = org.apache.logging.log4j.LogManager.getLogger(QueryLogging.QUERY_LOGGER_NAME);
            // The processor registers its queue gauges on the first record, so emit one before and after each reload.
            queryLogger.info("before reload");
            supplier.reload();
            queryLogger.info("after first reload");
            supplier.reload();
            queryLogger.info("after second reload");
            assertTrue("log export flush did not finish", supplier.forceFlush().join(15, TimeUnit.SECONDS).isDone());

            MetricData capacity = reader.collectAllMetrics()
                .stream()
                .filter(m -> m.getName().equals("otel.sdk.processor.log.queue.capacity"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("otel.sdk.processor.log.queue.capacity was not recorded"));
            assertThat(capacity.getLongSumData().getPoints(), hasSize(1));
            assertThat(capacity.getLongSumData().getPoints().iterator().next().getValue(), equalTo(42L));
        } finally {
            supplier.close();
            removeQueryLogger.run();
            meterProvider.close();
        }
    }

    /**
     * The query logger is not in the test log4j config. Add it before {@link OtelSdkExportLogsSupplier#install()} so the
     * appender is not attached to root. Returns a callback that removes it again.
     */
    private static Runnable addQueryLogger() {
        LoggerContext ctx = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        config.addLogger(
            QueryLogging.QUERY_LOGGER_NAME,
            LoggerConfig.newBuilder()
                .withConfig(config)
                .withLoggerName(QueryLogging.QUERY_LOGGER_NAME)
                .withLevel(Level.INFO)
                .withAdditivity(false)
                .build()
        );
        ctx.updateLoggers();
        return () -> {
            config.removeLogger(QueryLogging.QUERY_LOGGER_NAME);
            ctx.updateLoggers();
        };
    }
}
