/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporterBuilder;
import io.opentelemetry.instrumentation.log4j.appender.v2_17.OpenTelemetryAppender;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.elasticsearch.common.settings.Settings;

import java.io.Closeable;

/**
 * Builds an {@link SdkLoggerProvider} that exports log records via OTLP/gRPC, then installs
 * it into the log4j {@link OpenTelemetryAppender} and programmatically attaches that appender
 * to the {@code LoggingAuditTrail} logger so audit events flow out via OTLP. Currently used
 * solely for audit log delivery; the attachment point is not fundamental to this class and
 * could be extended to other loggers.
 *
 * <p>The appender is attached programmatically rather than via {@code log4j2.properties} because
 * log4j2 config files are parsed at JVM startup, before plugin/module classloaders are available;
 * the {@code OpenTelemetryAppender} plugin class is not on the boot classloader, so log4j cannot
 * resolve it from a config file. Doing it programmatically here means the appender is created
 * after this module's classloader is in scope, sidestepping the discovery issue.
 *
 * <p>gRPC (not HTTP) is required by the otel-delivery-gateway: HTTP clients reuse long-lived
 * connections, leading to uneven load distribution behind Kubernetes services.
 */
public class OtelSdkExportLogsSupplier implements Closeable {

    private static final Logger logger = LogManager.getLogger(OtelSdkExportLogsSupplier.class);

    /** Logger name that {@code LoggingAuditTrail} (in :x-pack:plugin:security) uses. */
    private static final String AUDIT_LOGGER_NAME = "org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail";

    private static final String OTEL_APPENDER_NAME = "audit_otel";

    private final Settings settings;
    private volatile SdkLoggerProvider loggerProvider;
    private volatile OpenTelemetryAppender attachedAppender;

    public OtelSdkExportLogsSupplier(Settings settings) {
        this.settings = settings;
    }

    /**
     * Build the {@link SdkLoggerProvider}, install it into {@link OpenTelemetryAppender}, and
     * attach a freshly-built appender to the audit logger. No-op if the feature is disabled.
     */
    public synchronized void install() {
        if (loggerProvider != null) {
            return;
        }
        if (OtelSdkSettings.TELEMETRY_LOGS_ENABLED.get(settings) == false) {
            return;
        }
        String endpoint = OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.get(settings);
        OtlpGrpcLogRecordExporterBuilder exporterBuilder = OtlpGrpcLogRecordExporter.builder()
            .setEndpoint(endpoint)
            .setTimeout(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.get(settings).toDuration())
            .setConnectTimeout(OtelSdkSettings.TELEMETRY_EXPORT_CONNECT_TIMEOUT.get(settings).toDuration())
            .setRetryPolicy(OtelSdkSettings.OTLP_RETRY_POLICY);
        String authHeader = OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings);
        if (authHeader != null) {
            exporterBuilder.addHeader("Authorization", authHeader);
        }

        int maxQueueSize = OtelSdkSettings.TELEMETRY_LOGS_MAX_QUEUE_SIZE.get(settings);
        SdkLoggerProvider provider = SdkLoggerProvider.builder()
            .setResource(OtelSdkResource.get(settings))
            .addLogRecordProcessor(BatchLogRecordProcessor.builder(exporterBuilder.build()).setMaxQueueSize(maxQueueSize).build())
            .build();

        OpenTelemetrySdk built = OpenTelemetrySdk.builder().setLoggerProvider(provider).build();

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig auditLoggerConfig = config.getLoggerConfig(AUDIT_LOGGER_NAME);
        if (AUDIT_LOGGER_NAME.equals(auditLoggerConfig.getName()) == false) {
            // No exact LoggerConfig for the audit logger (e.g. audit logging disabled). Bail.
            built.close();
            logger.warn("Audit logger config not found; skipping OTel logs install");
            return;
        }
        // Set the OpenTelemetry instance directly on the builder rather than via the static
        // OpenTelemetryAppender.install(...) — install() iterates registered appenders, which is
        // brittle when we're constructing one programmatically. setCaptureMapMessageAttributes
        // makes the StringMapMessage entries that LoggingAuditTrail emits surface as OTLP
        // attributes (otherwise only the formatted body is captured).
        OpenTelemetryAppender appender = OpenTelemetryAppender.builder()
            .setName(OTEL_APPENDER_NAME)
            .setOpenTelemetry(built)
            .setCaptureMapMessageAttributes(true)
            .build();
        appender.start();
        config.addAppender(appender);
        auditLoggerConfig.addAppender(appender, null, null);
        ctx.updateLoggers();

        this.loggerProvider = provider;
        this.attachedAppender = appender;
        logger.info("OTel SDK logs export installed; endpoint={}", endpoint);
    }

    /**
     * Force an immediate flush of any buffered log records through the {@code BatchLogRecordProcessor}
     * to the exporter. Returns the {@link CompletableResultCode} so the caller can join it
     * concurrently with other flush operations.
     */
    public CompletableResultCode forceFlush() {
        SdkLoggerProvider lp = loggerProvider;
        return lp != null ? lp.forceFlush() : CompletableResultCode.ofSuccess();
    }

    /** Returns {@code true} if {@link #install()} has been called and the OTel SDK is active. */
    public boolean isInstalled() {
        return loggerProvider != null;
    }

    @Override
    public synchronized void close() {
        detachAppender();
        if (loggerProvider != null) {
            loggerProvider.close();
            loggerProvider = null;
        }
    }

    /** Remove the OTel appender from the audit logger and stop it. */
    private void detachAppender() {
        if (attachedAppender == null) {
            return;
        }
        OpenTelemetryAppender appender = attachedAppender;
        attachedAppender = null;
        try {
            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration config = ctx.getConfiguration();
            LoggerConfig auditLoggerConfig = config.getLoggerConfig(AUDIT_LOGGER_NAME);
            if (AUDIT_LOGGER_NAME.equals(auditLoggerConfig.getName())) {
                auditLoggerConfig.removeAppender(OTEL_APPENDER_NAME);
            }
            config.getAppenders().remove(OTEL_APPENDER_NAME);
            ctx.updateLoggers();
            appender.stop();
        } catch (Exception e) {
            logger.warn("Error detaching OTel appender during close", e);
        }
    }
}
