/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporterBuilder;
import io.opentelemetry.instrumentation.log4j.appender.v2_17.OpenTelemetryAppender;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.resources.Resource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import java.io.Closeable;

/**
 * Builds an {@link SdkLoggerProvider} that exports log records via OTLP/HTTP and installs
 * it into the log4j {@link OpenTelemetryAppender}, so events emitted to log4j (e.g. audit
 * events from {@code LoggingAuditTrail}) are also delivered as OTLP log records.
 *
 * <p>POC scope: HTTP transport only, no mTLS, default {@link BatchLogRecordProcessor} settings.
 */
public class OtelSdkExportLogsSupplier implements Closeable {

    private static final Logger logger = LogManager.getLogger(OtelSdkExportLogsSupplier.class);

    private final Settings settings;
    private volatile OpenTelemetrySdk sdk;
    private volatile SdkLoggerProvider loggerProvider;

    public OtelSdkExportLogsSupplier(Settings settings) {
        this.settings = settings;
    }

    /**
     * Build the {@link SdkLoggerProvider} and install it into the log4j {@link OpenTelemetryAppender}.
     * No-op if {@code telemetry.otel.logs.enabled} is false.
     */
    public synchronized void install() {
        if (sdk != null) {
            return;
        }
        if (OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENABLED.get(settings) == false) {
            return;
        }
        String endpoint = OtelSdkSettings.TELEMETRY_OTEL_LOGS_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalStateException(
                "telemetry.otel.logs.enabled=true requires telemetry.otel.logs.endpoint to be configured"
            );
        }

        OtlpHttpLogRecordExporterBuilder exporterBuilder = OtlpHttpLogRecordExporter.builder().setEndpoint(endpoint);
        String authHeader = OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings);
        if (authHeader != null) {
            exporterBuilder.addHeader("Authorization", authHeader);
        }

        SdkLoggerProvider provider = SdkLoggerProvider.builder()
            .setResource(Resource.builder().put("service.name", "elasticsearch").build())
            .addLogRecordProcessor(BatchLogRecordProcessor.builder(exporterBuilder.build()).build())
            .build();

        OpenTelemetrySdk built = OpenTelemetrySdk.builder().setLoggerProvider(provider).build();
        OpenTelemetryAppender.install(built);

        this.loggerProvider = provider;
        this.sdk = built;
        logger.info("OTel SDK logs export installed; endpoint={}", endpoint);
    }

    @Override
    public synchronized void close() {
        if (loggerProvider != null) {
            loggerProvider.close();
            loggerProvider = null;
        }
        sdk = null;
    }
}
