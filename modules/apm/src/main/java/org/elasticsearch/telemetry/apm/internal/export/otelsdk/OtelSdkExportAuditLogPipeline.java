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
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages the OTel audit-log export pipeline: programmatically attaches an
 * {@link OpenTelemetryAppender} to the {@code LoggingAuditTrail} logger and exports the captured
 * records via OTLP/gRPC to the otel-delivery-gateway. Handles the full lifecycle — installation,
 * TLS certificate hot-reload, flush, and shutdown.
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
public class OtelSdkExportAuditLogPipeline implements Closeable {

    private static final Logger logger = LogManager.getLogger(OtelSdkExportAuditLogPipeline.class);

    /** Logger name that {@code LoggingAuditTrail} (in :x-pack:plugin:security) uses. */
    private static final String AUDIT_LOGGER_NAME = "org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail";

    private static final String OTEL_APPENDER_NAME = "audit_otel";

    private final Settings settings;
    private final Path configDir;
    private volatile OpenTelemetryAppender attachedAppender;

    /**
     * The same {@link SdkLoggerProvider} we used for {@link #attachedAppender}.
     * We must keep our own reference to this because we can't get it back from {@link #attachedAppender},
     * and we need it for {@link #forceFlush()} and {@link #close()}.
     * <p>
     * Null indicates that installation did not complete successfully (feature disabled, or audit
     * {@code LoggerConfig} absent).
     */
    private volatile SdkLoggerProvider loggerProvider;

    public OtelSdkExportAuditLogPipeline(Settings settings, Path configDir) {
        this.settings = settings;
        this.configDir = configDir;
        if (OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED.get(settings) == false) {
            return;
        }
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig auditLoggerConfig = config.getLoggerConfig(AUDIT_LOGGER_NAME);
        if (AUDIT_LOGGER_NAME.equals(auditLoggerConfig.getName()) == false) {
            // No exact LoggerConfig for the audit logger (e.g. audit logging disabled). Bail.
            logger.warn("Audit logger config not found; skipping OTel logs installation");
            return;
        }
        SdkLoggerProvider provider = buildLoggerProvider();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setLoggerProvider(provider).build();

        // Set the OpenTelemetry instance directly on the builder rather than via the static
        // OpenTelemetryAppender.install(...) — install() iterates registered appenders, which is
        // brittle when we're constructing one programmatically. setCaptureMapMessageAttributes
        // makes the StringMapMessage entries that LoggingAuditTrail emits surface as OTLP
        // attributes (otherwise only the formatted body is captured).
        OpenTelemetryAppender appender = OpenTelemetryAppender.builder()
            .setName(OTEL_APPENDER_NAME)
            .setOpenTelemetry(sdk)
            .setCaptureMapMessageAttributes(true)
            .build();
        appender.start();
        config.addAppender(appender);
        auditLoggerConfig.addAppender(appender, null, null);
        ctx.updateLoggers();

        this.loggerProvider = provider;
        this.attachedAppender = appender;
        logger.info("OTel SDK logs export installed; endpoint={}", OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENDPOINT.get(settings));
    }

    boolean isInstalled() {
        return loggerProvider != null;
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

    /**
     * Register {@link FileWatcher}s on the TLS cert, key, and CA files so the OTel logs export
     * automatically rebuilds when {@code elasticsearch-controller} rotates certificates in-place.
     * No-op when no SSL settings are configured or installation did not complete. Must be called
     * after construction.
     */
    public void initCertReload(ResourceWatcherService resourceWatcher) {
        if (loggerProvider == null) {
            // Installation did not complete: feature is disabled, or audit LoggerConfig absent.
            // No point watching cert files if we're not running.
            return;
        }
        List<String> cas = OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE_AUTHORITIES.get(settings);
        String cert = OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE.get(settings);
        String key = OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_KEY.get(settings);
        if (cas.isEmpty() && cert.isEmpty() && key.isEmpty()) {
            return;
        }
        FileChangesListener listener = new FileChangesListener() {
            @Override
            public void onFileCreated(Path file) {
                reload();
            }

            @Override
            public void onFileChanged(Path file) {
                reload();
            }

            @Override
            public void onFileDeleted(Path file) {
                reload();
            }
        };
        List<Path> watchPaths = new ArrayList<>(cas.size() + 2);
        for (String ca : cas) {
            watchPaths.add(resolvePath(ca));
        }
        if (cert.isEmpty() == false) {
            watchPaths.add(resolvePath(cert));
            watchPaths.add(resolvePath(key));
        }
        for (Path path : watchPaths) {
            FileWatcher watcher = new FileWatcher(path);
            watcher.addListener(listener);
            try {
                resourceWatcher.add(watcher, ResourceWatcherService.Frequency.HIGH);
            } catch (IOException e) {
                logger.warn("Cannot watch TLS file [{}]; cert hot-reload disabled for this file", path, e);
            }
        }
    }

    /** Build a fresh {@link SdkLoggerProvider} from the current settings (endpoint, auth, TLS). */
    private SdkLoggerProvider buildLoggerProvider() {
        OtlpGrpcLogRecordExporterBuilder exporterBuilder = OtlpGrpcLogRecordExporter.builder()
            .setEndpoint(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENDPOINT.get(settings));
        String authHeader = OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings);
        if (authHeader != null) {
            exporterBuilder.addHeader("Authorization", authHeader);
        }
        OtelSdkLogsTls tls = OtelSdkLogsTls.fromSettings(settings, configDir);
        if (tls != null) {
            exporterBuilder.setSslContext(tls.sslContext(), tls.trustManager());
        }
        return SdkLoggerProvider.builder()
            .setResource(OtelSdkResource.get(settings))
            .addLogRecordProcessor(BatchLogRecordProcessor.builder(exporterBuilder.build()).build())
            .build();
    }

    /**
     * Rebuild the OTel logs export with fresh TLS material, swapping into the running appender
     * atomically to avoid dropped records.
     *
     * <p>{@link OpenTelemetryAppender#setOpenTelemetry} is a volatile write guarded by a
     * {@code ReadWriteLock} inside the appender, so new audit events switch to the new channel
     * with no gap. The old {@link SdkLoggerProvider} is closed after the swap: its
     * {@code BatchLogRecordProcessor} flushes any buffered records through the still-valid old
     * channel (rotation happens before expiry) before shutting down the old gRPC connection.
     */
    private synchronized void reload() {
        logger.info("TLS cert files changed; reloading OTel logs export with new certificates");
        SdkLoggerProvider newProvider = buildLoggerProvider();
        attachedAppender.setOpenTelemetry(OpenTelemetrySdk.builder().setLoggerProvider(newProvider).build());
        SdkLoggerProvider oldProvider = loggerProvider;
        loggerProvider = newProvider;
        oldProvider.close();
        logger.info("OTel SDK logs export reloaded; endpoint={}", OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENDPOINT.get(settings));
    }

    private Path resolvePath(String pathStr) {
        Path p = Path.of(pathStr);
        return p.isAbsolute() ? p : configDir.resolve(p);
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
