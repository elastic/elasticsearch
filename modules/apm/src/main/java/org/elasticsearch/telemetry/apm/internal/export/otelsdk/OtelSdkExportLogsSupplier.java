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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
    private final Path configDir;
    private volatile SdkLoggerProvider loggerProvider;
    private volatile OpenTelemetryAppender attachedAppender;

    public OtelSdkExportLogsSupplier(Settings settings, Path configDir) {
        this.settings = settings;
        this.configDir = configDir;
    }

    /**
     * Build the {@link SdkLoggerProvider}, install it into {@link OpenTelemetryAppender}, and
     * attach a freshly-built appender to the audit logger. No-op if the feature is disabled.
     */
    public synchronized void install() {
        if (loggerProvider != null) {
            return;
        }
        if (OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED.get(settings) == false) {
            return;
        }
        String cert = OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE.get(settings);
        String key = OtelSdkSettings.TELEMETRY_LOGS_SSL_KEY.get(settings);
        if (cert.isEmpty() != key.isEmpty()) {
            throw new IllegalArgumentException(
                OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE.getKey()
                    + " and "
                    + OtelSdkSettings.TELEMETRY_LOGS_SSL_KEY.getKey()
                    + " must be set together"
            );
        }
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig auditLoggerConfig = config.getLoggerConfig(AUDIT_LOGGER_NAME);
        if (AUDIT_LOGGER_NAME.equals(auditLoggerConfig.getName()) == false) {
            // No exact LoggerConfig for the audit logger (e.g. audit logging disabled). Bail.
            logger.warn("Audit logger config not found; skipping OTel logs install");
            return;
        }

        SdkLoggerProvider provider = buildProvider();
        // Set the OpenTelemetry instance directly on the builder rather than via the static
        // OpenTelemetryAppender.install(...) — install() iterates registered appenders, which is
        // brittle when we're constructing one programmatically. setCaptureMapMessageAttributes
        // makes the StringMapMessage entries that LoggingAuditTrail emits surface as OTLP
        // attributes (otherwise only the formatted body is captured).
        OpenTelemetryAppender appender = OpenTelemetryAppender.builder()
            .setName(OTEL_APPENDER_NAME)
            .setOpenTelemetry(OpenTelemetrySdk.builder().setLoggerProvider(provider).build())
            .setCaptureMapMessageAttributes(true)
            .build();
        appender.start();
        config.addAppender(appender);
        auditLoggerConfig.addAppender(appender, null, null);
        ctx.updateLoggers();

        this.loggerProvider = provider;
        this.attachedAppender = appender;
        logger.info("OTel SDK logs export installed; endpoint={}", OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.get(settings));
    }

    /**
     * Register {@link FileWatcher}s on the TLS cert, key, and CA files so the OTel logs export
     * automatically rebuilds its gRPC connection when the controller rotates certificates in-place.
     * No-op when no SSL settings are configured or installation did not complete (feature disabled,
     * or audit {@code LoggerConfig} absent). Must be called after {@link #install()}.
     */
    public void initCertReload(ResourceWatcherService resourceWatcher) {
        if (loggerProvider == null) {
            return;
        }
        List<String> cas = OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE_AUTHORITIES.get(settings);
        String cert = OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE.get(settings);
        String key = OtelSdkSettings.TELEMETRY_LOGS_SSL_KEY.get(settings);
        if (cas.isEmpty() && cert.isEmpty()) {
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

    /**
     * Build a fresh {@link SdkLoggerProvider} from the current settings, including reading TLS
     * material from disk. Used both at initial install and during cert hot-reload.
     */
    private SdkLoggerProvider buildProvider() {
        String cert = OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE.get(settings);
        String key = OtelSdkSettings.TELEMETRY_LOGS_SSL_KEY.get(settings);
        OtlpGrpcLogRecordExporterBuilder exporterBuilder = OtlpGrpcLogRecordExporter.builder()
            .setEndpoint(OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.get(settings))
            .setTimeout(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.get(settings).toDuration())
            .setConnectTimeout(OtelSdkSettings.TELEMETRY_EXPORT_CONNECT_TIMEOUT.get(settings).toDuration())
            .setRetryPolicy(OtelSdkSettings.OTLP_RETRY_POLICY);
        String authHeader = OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings);
        if (authHeader != null) {
            exporterBuilder.addHeader("Authorization", authHeader);
        }
        List<String> cas = OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE_AUTHORITIES.get(settings);
        if (cas.isEmpty() == false) {
            exporterBuilder.setTrustedCertificates(readPemFiles(cas));
        }
        if (cert.isEmpty() == false) {
            exporterBuilder.setClientTls(readPemFile(key), readPemFile(cert));
        }
        int maxQueueSize = OtelSdkSettings.TELEMETRY_LOGS_MAX_QUEUE_SIZE.get(settings);
        return SdkLoggerProvider.builder()
            .setResource(OtelSdkResource.get(settings))
            .addLogRecordProcessor(BatchLogRecordProcessor.builder(exporterBuilder.build()).setMaxQueueSize(maxQueueSize).build())
            .build();
    }

    /**
     * Rebuild the OTel logs export with fresh TLS material and swap it into the running appender
     * atomically to avoid dropped records.
     *
     * <p>{@link OpenTelemetryAppender#setOpenTelemetry} is a volatile write guarded by a
     * {@code ReadWriteLock} inside the appender, so new audit events switch to the new channel
     * without a gap. The old {@link SdkLoggerProvider} is closed after the swap: its
     * {@code BatchLogRecordProcessor} flushes any buffered records through the still-valid old
     * channel (rotation happens before cert expiry) before shutting down the old gRPC connection.
     */
    private synchronized void reload() {
        if (loggerProvider == null) {
            return;
        }
        logger.info("TLS cert files changed; reloading OTel logs export with new certificates");
        SdkLoggerProvider newProvider = buildProvider();
        attachedAppender.setOpenTelemetry(OpenTelemetrySdk.builder().setLoggerProvider(newProvider).build());
        SdkLoggerProvider oldProvider = loggerProvider;
        loggerProvider = newProvider;
        oldProvider.close();
        logger.info("OTel SDK logs export reloaded; endpoint={}", OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.get(settings));
    }

    private byte[] readPemFile(String pathStr) {
        try {
            return Files.readAllBytes(resolvePath(pathStr));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read TLS file: " + pathStr, e);
        }
    }

    private byte[] readPemFiles(List<String> paths) {
        int total = 0;
        byte[][] parts = new byte[paths.size()][];
        for (int i = 0; i < paths.size(); i++) {
            parts[i] = readPemFile(paths.get(i));
            total += parts[i].length;
        }
        byte[] result = new byte[total];
        int offset = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, result, offset, part.length);
            offset += part.length;
        }
        return result;
    }

    private Path resolvePath(String pathStr) {
        Path p = Path.of(pathStr);
        return p.isAbsolute() ? p : configDir.resolve(p);
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
