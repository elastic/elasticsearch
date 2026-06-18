/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.common.settings.Setting.Property.OperatorDynamic;

/**
 * Node settings for the OpenTelemetry SDK metrics ({@link OtelSdkExportMeterSupplier}), traces
 * ({@link OtelSdkExportTracerSupplier}), and audit logs ({@link OtelSdkExportAuditLogPipeline}) export paths.
 */
public final class OtelSdkSettings {

    private OtelSdkSettings() {}

    public static final Setting<String> TELEMETRY_OTEL_METRICS_ENDPOINT = Setting.simpleString(
        "telemetry.otel.metrics.endpoint",
        "",
        NodeScope
    );

    public static final Setting<TimeValue> TELEMETRY_OTEL_METRICS_INTERVAL = Setting.timeSetting(
        "telemetry.otel.metrics.interval",
        TimeValue.timeValueSeconds(10),
        NodeScope
    );

    public static final Setting<Boolean> TELEMETRY_OTEL_METRICS_ENABLED = Setting.boolSetting(
        "telemetry.otel.metrics.enabled",
        false,
        NodeScope
    );

    /** Disk cap for buffered batches while OTLP is unreachable. {@code 0b} disables buffering. */
    public static final Setting<ByteSizeValue> TELEMETRY_OTEL_METRICS_DISK_BUFFER_SIZE = Setting.byteSizeSetting(
        "telemetry.otel.metrics.disk_buffer_size",
        ByteSizeValue.ofMb(512),
        ByteSizeValue.ZERO,
        ByteSizeValue.ofBytes(Integer.MAX_VALUE),
        NodeScope
    );

    /** Buffered entries older than this are dropped. */
    public static final Setting<TimeValue> TELEMETRY_OTEL_METRICS_BUFFER_TTL = Setting.timeSetting(
        "telemetry.otel.metrics.buffer_ttl",
        TimeValue.timeValueHours(12),
        NodeScope
    );

    /**
     * How long the current write file is kept open before rotating to a new one. Maps to
     * {@code FileStorageConfiguration.maxFileAgeForWriteMillis}; must be strictly less than
     * {@link #TELEMETRY_OTEL_METRICS_DISK_BUFFER_READ_MIN_AGE} or the library rejects the config.
     */
    public static final Setting<TimeValue> TELEMETRY_OTEL_METRICS_DISK_BUFFER_WRITE_WINDOW = Setting.timeSetting(
        "telemetry.otel.metrics.disk_buffer_write_window",
        TimeValue.timeValueSeconds(30),
        NodeScope
    );

    /**
     * Minimum age before a buffered file is eligible for replay. Maps to
     * {@code FileStorageConfiguration.minFileAgeForReadMillis}; must be strictly greater than
     * {@link #TELEMETRY_OTEL_METRICS_DISK_BUFFER_WRITE_WINDOW}.
     */
    public static final Setting<TimeValue> TELEMETRY_OTEL_METRICS_DISK_BUFFER_READ_MIN_AGE = Setting.timeSetting(
        "telemetry.otel.metrics.disk_buffer_read_min_age",
        TimeValue.timeValueSeconds(33),
        NodeScope
    );

    /** Total attempts per export (initial + retries). {@code 1} disables retry. */
    public static final Setting<Integer> TELEMETRY_OTEL_OTLP_RETRY_MAX_ATTEMPTS = Setting.intSetting(
        "telemetry.otel.otlp.retry.max_attempts",
        2,
        1,
        5,
        NodeScope
    );

    public static final Setting<TimeValue> TELEMETRY_OTEL_OTLP_RETRY_INITIAL_BACKOFF = Setting.timeSetting(
        "telemetry.otel.otlp.retry.initial_backoff",
        TimeValue.timeValueSeconds(1),
        NodeScope
    );

    public static final Setting<Double> TELEMETRY_OTEL_OTLP_RETRY_BACKOFF_MULTIPLIER = Setting.doubleSetting(
        "telemetry.otel.otlp.retry.backoff_multiplier",
        1.5,
        1.0,
        NodeScope
    );

    /**
     * Total deadline for one OTLP send() including retries; must stay below the collection interval so a slow export
     * does not stretch into the next cycle.
     */
    public static final Setting<TimeValue> TELEMETRY_OTEL_OTLP_SEND_TIMEOUT = Setting.timeSetting(
        "telemetry.otel.otlp.send_timeout",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueSeconds(60),
        NodeScope
    );

    public static final Setting<TimeValue> TELEMETRY_OTEL_OTLP_CONNECT_TIMEOUT = Setting.timeSetting(
        "telemetry.otel.otlp.connect_timeout",
        TimeValue.timeValueSeconds(2),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueSeconds(10),
        NodeScope
    );

    /** OTLP HTTP endpoint URL where the SDK exports buffered spans. Required when the SDK trace path is active. */
    public static final Setting<String> TELEMETRY_OTEL_TRACES_ENDPOINT = Setting.simpleString(
        "telemetry.otel.traces.endpoint",
        "",
        NodeScope
    );

    /** How often {@code BatchSpanProcessor} flushes buffered spans to the exporter. */
    public static final Setting<TimeValue> TELEMETRY_OTEL_TRACES_INTERVAL = Setting.timeSetting(
        "telemetry.otel.traces.interval",
        // Matches the APM agent's api_request_time default; full batches (512 spans) flush immediately.
        TimeValue.timeValueSeconds(10),
        NodeScope
    );

    /** Maximum depth of child spans per request. {@code 0} exports only the root span.
     * Spans from an upstream {@code traceparent} are not counted. */
    public static final Setting<Integer> TELEMETRY_OTEL_TRACES_MAX_TRACE_DEPTH = Setting.intSetting(
        "telemetry.otel.traces.max_trace_depth",
        0,
        0,
        OperatorDynamic,
        NodeScope
    );

    /** Per-trace sample rate applied to locally-started traces.*/
    public static final Setting<Double> TELEMETRY_OTEL_TRACES_SAMPLE_RATE = Setting.doubleSetting(
        "telemetry.otel.traces.sample_rate",
        0.001,
        0.0,
        1.0,
        NodeScope
    );

    /** Maximum number of spans the {@code BatchSpanProcessor} buffers before dropping.*/
    public static final Setting<Integer> TELEMETRY_OTEL_TRACES_BATCH_MAX_QUEUE_SIZE = Setting.intSetting(
        "telemetry.otel.traces.batch.max_queue_size",
        1024,
        1,
        NodeScope
    );

    /** Maximum number of spans exported per OTLP batch. Must be {@code <= max_queue_size}. */
    public static final Setting<Integer> TELEMETRY_OTEL_TRACES_BATCH_MAX_EXPORT_BATCH_SIZE = Setting.intSetting(
        "telemetry.otel.traces.batch.max_export_batch_size",
        512,
        1,
        NodeScope
    );

    /** Per-batch deadline the {@code BatchSpanProcessor} gives the exporter before timing out. */
    public static final Setting<TimeValue> TELEMETRY_OTEL_TRACES_BATCH_EXPORT_TIMEOUT = Setting.timeSetting(
        "telemetry.otel.traces.batch.export_timeout",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(1),
        NodeScope
    );

    /**
     * When {@code true}, exceptions recorded fully on a span are attached via {@link io.opentelemetry.api.trace.Span#recordException}.
     * When {@code false}, only {@code exception.type} and {@code exception.message} are emitted as an {@code exception} span event.
     */
    public static final Setting<Boolean> TELEMETRY_OTEL_TRACES_RECORD_EXCEPTION_STACKS = Setting.boolSetting(
        "telemetry.otel.traces.record_exception_stacks",
        false,
        OperatorDynamic,
        NodeScope
    );

    /** Best-effort upper bound on time spent flushing buffered metrics or spans to the exporter. */
    public static final Setting<TimeValue> TELEMETRY_OTEL_FLUSH_TIMEOUT = Setting.timeSetting(
        "telemetry.otel.flush_timeout",
        TimeValue.timeValueSeconds(10),
        NodeScope
    );

    /** External OTel resource attributes attached to every metric and span exported by the SDK path.*/
    public static final Setting.AffixSetting<String> TELEMETRY_OTEL_RESOURCE_ATTRIBUTES = Setting.prefixKeySetting(
        "telemetry.otel.resource.",
        key -> Setting.simpleString(key, NodeScope)
    );

    /** OTLP/gRPC endpoint URL where the SDK exports audit log records. Required when {@link #TELEMETRY_LOGS_AUDIT_ENABLED} is true. */
    public static final Setting<String> TELEMETRY_LOGS_AUDIT_ENDPOINT = Setting.simpleString(
        "telemetry.logs.audit.endpoint",
        "",
        NodeScope
    );

    /**
     * PEM-encoded CA certificate(s) used to verify the otel-delivery-gateway's server certificate.
     * These CAs are merged with the JVM default trust store so that gateway chains whose root is
     * already in the JVM store are validated correctly (the multi-parent-chain fix).
     * Paths are relative to the Elasticsearch config directory.
     * When empty, the JVM default CAs are used and no client certificate is sent.
     */
    public static final Setting<List<String>> TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE_AUTHORITIES = Setting.stringListSetting(
        "telemetry.logs.audit.ssl.certificate_authorities",
        NodeScope
    );

    /**
     * Path (relative to the Elasticsearch config directory) to the PEM-encoded client certificate
     * that Elasticsearch presents to the otel-delivery-gateway during mTLS.
     * Must be set together with {@link #TELEMETRY_LOGS_AUDIT_SSL_KEY}.
     */
    public static final Setting<String> TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE = Setting.simpleString(
        "telemetry.logs.audit.ssl.certificate",
        "",
        NodeScope
    );

    /**
     * Path (relative to the Elasticsearch config directory) to the PEM-encoded private key
     * for the client certificate ({@link #TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE}).
     * Must be set together with {@link #TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE}.
     * The key must be unencrypted; the elasticsearch-controller delivers plain PEM keys.
     */
    public static final Setting<String> TELEMETRY_LOGS_AUDIT_SSL_KEY = Setting.simpleString("telemetry.logs.audit.ssl.key", "", NodeScope);

    /** Whether the OTel SDK audit-log export path is active. When false, {@link OtelSdkExportAuditLogPipeline} installs nothing. */
    public static final Setting<Boolean> TELEMETRY_LOGS_AUDIT_ENABLED = Setting.boolSetting(
        "telemetry.logs.audit.enabled",
        false,
        new Setting.Validator<>() {
            @Override
            public void validate(Boolean value) {}

            @Override
            public void validate(Boolean value, Map<Setting<?>, Object> settings) {
                if (value && ((String) settings.get(TELEMETRY_LOGS_AUDIT_ENDPOINT)).isEmpty()) {
                    throw new IllegalArgumentException(
                        TELEMETRY_LOGS_AUDIT_ENDPOINT.getKey() + " must be configured when telemetry.logs.audit.enabled=true"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(TELEMETRY_LOGS_AUDIT_ENDPOINT).iterator();
            }
        },
        NodeScope
    );
}
