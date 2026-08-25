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
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.DefaultJdkTrustConfig;
import org.elasticsearch.common.ssl.PemKeyConfig;
import org.elasticsearch.common.ssl.PemTrustConfig;
import org.elasticsearch.common.ssl.SslTrustConfig;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.TelemetryLogEventFilter;
import org.elasticsearch.telemetry.TelemetryLoggingFilterProvider;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * Builds an {@link SdkLoggerProvider} that exports log records via OTLP/gRPC, then installs
 * it into the log4j appender ({@link ElasticsearchOtelAppender}) and programmatically attaches that appender
 * to the {@code LoggingAuditTrail} logger so audit events flow out via OTLP. Currently used
 * solely for audit log delivery; the attachment point is not fundamental to this class and
 * could be extended to other loggers.
 *
 * <p>The appender is attached programmatically rather than via {@code log4j2.properties} because
 * log4j2 config files are parsed at JVM startup, before plugin/module classloaders are available;
 * the {@code ElasticsearchOtelAppender} plugin class is not on the boot classloader, so log4j cannot
 * resolve it from a config file. Doing it programmatically here means the appender is created
 * after this module's classloader is in scope, sidestepping the discovery issue.
 * FIXME: verify the above is still true?
 *
 * <p>gRPC (not HTTP) is required by the otel-delivery-gateway: HTTP clients reuse long-lived
 * connections, leading to uneven load distribution behind Kubernetes services.
 */
public class OtelSdkExportLogsSupplier implements Closeable {

    private static final Logger logger = LogManager.getLogger(OtelSdkExportLogsSupplier.class);

    /** Logger name that {@code LoggingAuditTrail} (in :x-pack:plugin:security) uses. */
    private static final String AUDIT_LOGGER_NAME = "org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail";

    public static final String OTEL_AUDIT_APPENDER_NAME = "audit_otel";
    public static final String OTEL_QUERYLOG_APPENDER_NAME = "querylog_otel";

    private final Settings settings;
    private final Path configDir;
    private final Collection<TelemetryLoggingFilterProvider> filterProviders;
    private volatile SdkLoggerProvider loggerProvider;
    private final List<Consumer<Configuration>> closeCallbacks = new ArrayList<>();
    private final List<ElasticsearchOtelAppender> appenders = new ArrayList<>();

    public OtelSdkExportLogsSupplier(Settings settings, Path configDir, Collection<TelemetryLoggingFilterProvider> filterProviders) {
        this.settings = settings;
        this.configDir = configDir;
        this.filterProviders = filterProviders;
    }

    // for tests and contexts with no filter providers
    public OtelSdkExportLogsSupplier(Settings settings, Path configDir) {
        this(settings, configDir, List.of());
    }

    @Nullable
    private TelemetryLogEventFilter selectAppenderFilters(String appenderName) {
        List<TelemetryLogEventFilter> filters = filterProviders.stream()
            .map(p -> p.getLogFilter(appenderName))
            .filter(Objects::nonNull)
            .toList();
        if (filters.isEmpty()) return null;
        if (filters.size() == 1) return filters.getFirst();
        return data -> {
            for (TelemetryLogEventFilter f : filters) {
                data = f.filter(data);
                if (data == null) return null;
            }
            return data;
        };
    }

    /**
     * Build the {@link SdkLoggerProvider}, for loggers that are enabled:
     * - install it into {@link ElasticsearchOtelAppender}, and
     * - attach a freshly-built appender to the logger.
     */
    public synchronized void install() {
        if (loggerProvider != null) {
            return;
        }
        if (OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED.get(settings) == false
            && OtelSdkSettings.TELEMETRY_LOGS_QUERYLOG_ENABLED.get(settings) == false) {
            // No telemetry enabled, exit.
            return;
        }
        loggerProvider = buildProvider();
        LoggerContext ctx = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        if (OtelSdkSettings.TELEMETRY_LOGS_AUDIT_ENABLED.get(settings)) {
            installAuditAppender(config);
        }
        if (OtelSdkSettings.TELEMETRY_LOGS_QUERYLOG_ENABLED.get(settings)) {
            installQuerylogAppender(config);
        }
        ctx.updateLoggers();
        logger.info("OTel SDK logs export installed; endpoint={}", OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.get(settings));
    }

    private void installQuerylogAppender(Configuration config) {
        LoggerConfig querylogConfig = config.getLoggerConfig(QueryLogging.QUERY_LOGGER_NAME);

        ElasticsearchOtelAppender querylogAppender = new ElasticsearchOtelAppender(
            OTEL_QUERYLOG_APPENDER_NAME,
            OpenTelemetrySdk.builder().setLoggerProvider(loggerProvider).build(),
            selectAppenderFilters(OTEL_QUERYLOG_APPENDER_NAME)
        );
        querylogAppender.start();
        config.addAppender(querylogAppender);
        querylogConfig.addAppender(querylogAppender, null, null);
        appenders.add(querylogAppender);
        closeCallbacks.add(c -> closeQuerylogAppender(c, querylogAppender));
    }

    private static void closeQuerylogAppender(Configuration config, ElasticsearchOtelAppender appender) {
        try {
            LoggerConfig querylogConfig = config.getLoggerConfig(QueryLogging.QUERY_LOGGER_NAME);
            if (QueryLogging.QUERY_LOGGER_NAME.equals(querylogConfig.getName())) {
                querylogConfig.removeAppender(OTEL_QUERYLOG_APPENDER_NAME);
            }
            config.getAppenders().remove(OTEL_QUERYLOG_APPENDER_NAME);
            appender.stop();
        } catch (Exception e) {
            logger.warn("Error detaching querylog OTel appender during close", e);
        }
    }

    private void installAuditAppender(Configuration config) {
        LoggerConfig auditLoggerConfig = config.getLoggerConfig(AUDIT_LOGGER_NAME);
        if (AUDIT_LOGGER_NAME.equals(auditLoggerConfig.getName()) == false) {
            // No exact LoggerConfig for the audit logger (e.g. audit logging disabled). Bail.
            logger.warn("Audit logger config not found; skipping OTel logs install");
            return;
        }

        // setCaptureMapMessageAttributes makes the StringMapMessage entries that LoggingAuditTrail emits surface as OTLP
        // attributes (otherwise only the formatted body is captured).
        ElasticsearchOtelAppender appender = new ElasticsearchOtelAppender(
            OTEL_AUDIT_APPENDER_NAME,
            OpenTelemetrySdk.builder().setLoggerProvider(loggerProvider).build(),
            selectAppenderFilters(OTEL_AUDIT_APPENDER_NAME)
        );
        appender.start();
        config.addAppender(appender);
        auditLoggerConfig.addAppender(appender, null, null);
        appenders.add(appender);
        closeCallbacks.add(c -> closeAuditAppender(c, appender));
    }

    private static void closeAuditAppender(Configuration config, ElasticsearchOtelAppender appender) {
        try {
            LoggerConfig auditLoggerConfig = config.getLoggerConfig(AUDIT_LOGGER_NAME);
            if (AUDIT_LOGGER_NAME.equals(auditLoggerConfig.getName())) {
                auditLoggerConfig.removeAppender(OTEL_AUDIT_APPENDER_NAME);
            }
            config.getAppenders().remove(OTEL_AUDIT_APPENDER_NAME);
            appender.stop();
        } catch (Exception e) {
            logger.warn("Error detaching audit OTel appender during close", e);
        }
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
        List<String> cas = OtelSdkSettings.TELEMETRY_LOGS_SSL_CERTIFICATE_AUTHORITIES.get(settings);
        if (cas.isEmpty() == false || cert.isEmpty() == false) {
            try {
                SslTrustConfig trustConfig = cas.isEmpty() ? DefaultJdkTrustConfig.DEFAULT_INSTANCE : new PemTrustConfig(cas, configDir);
                X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
                KeyManager[] keyManagers = null;
                if (cert.isEmpty() == false) {
                    keyManagers = new KeyManager[] { new PemKeyConfig(cert, key, new char[0], configDir).createKeyManager() };
                }
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagers, new TrustManager[] { trustManager }, null);
                exporterBuilder.setSslContext(sslContext, trustManager);
            } catch (GeneralSecurityException e) {
                throw new RuntimeException("Failed to initialise TLS context for OTel log export", e);
            }
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
     * <p>{@link ElasticsearchOtelAppender#setOpenTelemetry} is a volatile write guarded by a
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
        var sdk = OpenTelemetrySdk.builder().setLoggerProvider(newProvider).build();
        appenders.forEach(appender -> appender.setOpenTelemetry(sdk));
        SdkLoggerProvider oldProvider = loggerProvider;
        loggerProvider = newProvider;
        oldProvider.close();
        logger.info("OTel SDK logs export reloaded; endpoint={}", OtelSdkSettings.TELEMETRY_LOGS_ENDPOINT.get(settings));
    }

    private Path resolvePath(String pathStr) {
        return configDir.resolve(pathStr);
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
        detachAppenders();
        if (loggerProvider != null) {
            loggerProvider.close();
            loggerProvider = null;
        }
    }

    /** Remove the OTel appender from the audit logger and stop it. */
    private void detachAppenders() {
        if (closeCallbacks.isEmpty()) {
            return;
        }
        LoggerContext ctx = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        closeCallbacks.forEach(cb -> cb.accept(config));
        closeCallbacks.clear();
        appenders.clear();
        ctx.updateLoggers();
    }
}
