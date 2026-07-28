/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.sdk.common.export.RetryPolicy;

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
 * ({@link OtelSdkExportTracerSupplier}), and logs ({@link OtelSdkExportLogsSupplier}) export paths.
 */
public final class OtelSdkSettings {

    private OtelSdkSettings() {}

    /**
     * Upper bound on the best-effort flush of buffered metrics and spans on shutdown, so an unreachable exporter cannot hang it.
     * 1 minute is a reasonable default that protects from pathological cases without being too restrictive.
     * */
    public static final TimeValue OTEL_EXPORT_FLUSH_TIMEOUT = TimeValue.timeValueMinutes(1);

    // --- Resource attributes (all signals)

    /** External OTel resource attributes attached to every metric, span and log record exported by the SDK path. */
    public static final Setting.AffixSetting<String> TELEMETRY_RESOURCE_ATTRIBUTES = Setting.prefixKeySetting(
        "telemetry.resource.",
        key -> Setting.simpleString(key, NodeScope)
    );

    // --- Shared OTLP export transport (metrics + traces)

    /** URL ({@code http://host:port}, no path) where the SDK exports metrics and spans.
     * Required when the SDK metrics or trace path is active. */
    public static final Setting<String> TELEMETRY_EXPORT_ENDPOINT = Setting.simpleString("telemetry.export.endpoint", "", NodeScope);

    /** When {@code false}, TLS certificate verification is disabled for the OTLP exporters. */
    public static final Setting<Boolean> TELEMETRY_EXPORT_VERIFY_SERVER_CERT = Setting.boolSetting(
        "telemetry.export.verify_server_cert",
        true,
        NodeScope
    );

    /**
     * Initial backoff of the shared OTLP retry policy ({@link #OTLP_RETRY_POLICY}).
     * The default allows for fast retry and manageable total timeout.
     * */
    public static final TimeValue OTLP_RETRY_INITIAL_BACKOFF = TimeValue.timeValueMillis(100);

    /**
     * Backoff multiplier of the shared OTLP retry policy ({@link #OTLP_RETRY_POLICY}).
     * The default allows the second retry to come with a reasonable distance from the first.
     * */
    public static final double OTLP_RETRY_BACKOFF_MULTIPLIER = 5;

    /** Retry policy shared by the metrics and traces OTLP exporters. */
    public static final RetryPolicy OTLP_RETRY_POLICY = RetryPolicy.builder()
        .setMaxAttempts(3)
        .setInitialBackoff(OTLP_RETRY_INITIAL_BACKOFF.toDuration())
        .setBackoffMultiplier(OTLP_RETRY_BACKOFF_MULTIPLIER)
        .build();

    /**
     * Total deadline for one OTLP send() including retries. Floored at {@link #OTLP_RETRY_INITIAL_BACKOFF} so a failing
     * send can still complete a retry within budget, and kept below the export interval so a slow export does not
     * stretch into the next cycle.
     **/
    public static final Setting<TimeValue> TELEMETRY_EXPORT_SEND_TIMEOUT = Setting.timeSetting(
        "telemetry.export.send_timeout",
        TimeValue.timeValueSeconds(5),
        OTLP_RETRY_INITIAL_BACKOFF,
        TimeValue.timeValueSeconds(60),
        NodeScope
    );

    public static final Setting<TimeValue> TELEMETRY_EXPORT_CONNECT_TIMEOUT = Setting.timeSetting(
        "telemetry.export.connect_timeout",
        TimeValue.timeValueSeconds(2),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueSeconds(10),
        NodeScope
    );

    /**
     * Export cadence: the {@code PeriodicMetricReader} interval and the {@code BatchSpanProcessor} schedule delay.
     * Defaults to the legacy {@code telemetry.agent.metrics_interval} when set, otherwise 60s. Must be greater than
     * {@link #TELEMETRY_EXPORT_SEND_TIMEOUT}.
     */
    public static final Setting<TimeValue> TELEMETRY_EXPORT_INTERVAL = Setting.timeSetting(
        "telemetry.export.interval",
        settings -> TimeValue.parseTimeValue(settings.get("telemetry.agent.metrics_interval", "60s"), "telemetry.export.interval"),
        new GreaterThanTimeValueValidator("telemetry.export.interval", TELEMETRY_EXPORT_SEND_TIMEOUT),
        NodeScope
    );

    // --- Metrics

    /** When {@code true}, also emit the OTel semantic-convention JVM metric names alongside the legacy APM-agent names,
     * to support migrating dashboards to the semconv names (ES-14386). */
    public static final Setting<Boolean> NODE_METRICS_OTEL_SEMCONV_ENABLED_SETTING = Setting.boolSetting(
        "node.metrics.otel_semconv.enabled",
        false,
        NodeScope
    );

    /** Disk cap for buffered batches while OTLP is unreachable. {@code 0b} disables buffering. */
    public static final Setting<ByteSizeValue> TELEMETRY_METRICS_BUFFER_DISK_SIZE = Setting.byteSizeSetting(
        "telemetry.metrics.buffer.disk_size",
        ByteSizeValue.ofMb(512),
        ByteSizeValue.ZERO,
        ByteSizeValue.ofBytes(Integer.MAX_VALUE),
        NodeScope
    );

    /** Buffered entries older than this are dropped. */
    public static final Setting<TimeValue> TELEMETRY_METRICS_BUFFER_TTL = Setting.timeSetting(
        "telemetry.metrics.buffer.ttl",
        TimeValue.timeValueHours(12),
        NodeScope
    );

    /**
     * Instrument name patterns whose metrics are dropped.
     * Patterns may contain the wildcards {@code *} (any run of characters) and {@code ?} (a single character).
     */
    public static final Setting<List<String>> TELEMETRY_METRICS_DISABLED = Setting.stringListSetting(
        "telemetry.metrics.disabled",
        NodeScope
    );

    /**
     * Whether the per-instrument collection-time histogram ({@code es.apm.metrics.instrument.collection_time.histogram}) is
     * recorded. Off by default because it adds one series per observable instrument; enable it at runtime to attribute metric
     * collection cost to a specific instrument, then disable it again.
     */
    public static final Setting<Boolean> TELEMETRY_METRICS_INSTRUMENT_TIMING_ENABLED = Setting.boolSetting(
        "telemetry.metrics.instrument_timing.enabled",
        false,
        OperatorDynamic,
        NodeScope
    );

    // --- Traces

    /** Maximum depth of child spans per request. {@code 0} exports only the root span.
     * Spans from an upstream {@code traceparent} are not counted. */
    public static final Setting<Integer> TELEMETRY_TRACING_MAX_DEPTH = Setting.intSetting(
        "telemetry.tracing.max_depth",
        0,
        0,
        OperatorDynamic,
        NodeScope
    );

    /** Per-trace sample rate applied to locally-started traces. Defaults to the legacy
     * {@code telemetry.agent.transaction_sample_rate} when set, otherwise 0.001. */
    public static final Setting<Double> TELEMETRY_TRACING_SAMPLE_RATE = new Setting<>(
        "telemetry.tracing.sample_rate",
        settings -> settings.get("telemetry.agent.transaction_sample_rate", "0.001"),
        s -> Setting.parseDouble(s, 0.0, 1.0, "telemetry.tracing.sample_rate", false),
        NodeScope
    );

    /** Maximum number of spans the {@code BatchSpanProcessor} buffers before dropping. Defaults to the legacy
     * {@code telemetry.agent.max_queue_size} when set, otherwise 1024. */
    public static final Setting<Integer> TELEMETRY_TRACING_MAX_QUEUE_SIZE = Setting.intSetting(
        "telemetry.tracing.max_queue_size",
        settings -> settings.get("telemetry.agent.max_queue_size", "1024"),
        1,
        Integer.MAX_VALUE,
        NodeScope
    );

    /** Maximum number of spans exported per OTLP batch. Must be {@code <= max_queue_size}. */
    public static final Setting<Integer> TELEMETRY_TRACING_MAX_BATCH_SIZE = Setting.intSetting(
        "telemetry.tracing.max_batch_size",
        512,
        1,
        NodeScope
    );

    /**
     * When {@code true}, exceptions recorded fully on a span are attached via {@link io.opentelemetry.api.trace.Span#recordException}.
     * When {@code false}, only {@code exception.type} and {@code exception.message} are emitted as an {@code exception} span event.
     */
    public static final Setting<Boolean> TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS = Setting.boolSetting(
        "telemetry.tracing.record_exception_stacks",
        false,
        OperatorDynamic,
        NodeScope
    );

    // --- Logs

    /** OTLP/gRPC endpoint URL where the SDK exports audit log records. Required when {@link #TELEMETRY_LOGS_AUDIT_ENABLED} is true. */
    public static final Setting<String> TELEMETRY_LOGS_ENDPOINT = Setting.simpleString("telemetry.logs.endpoint", "", NodeScope);

    /** Whether the OTel SDK audit-log export path is active. When false, {@link OtelSdkExportLogsSupplier} installs nothing. */
    public static final Setting<Boolean> TELEMETRY_LOGS_AUDIT_ENABLED = Setting.boolSetting(
        "telemetry.logs.audit.enabled",
        false,
        new Setting.Validator<>() {
            @Override
            public void validate(Boolean value) {}

            @Override
            public void validate(Boolean value, Map<Setting<?>, Object> settings) {
                if (value && ((String) settings.get(TELEMETRY_LOGS_ENDPOINT)).isEmpty()) {
                    throw new IllegalArgumentException(
                        TELEMETRY_LOGS_ENDPOINT.getKey() + " must be configured when telemetry.logs.audit.enabled=true"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(TELEMETRY_LOGS_ENDPOINT).iterator();
            }
        },
        NodeScope
    );

    /**
     * Maximum number of log records the {@code BatchLogRecordProcessor} buffers before dropping.
     * Sized for ~30 MB of in-flight records at ~3 KB/record average.
     */
    public static final Setting<Integer> TELEMETRY_LOGS_MAX_QUEUE_SIZE = Setting.intSetting(
        "telemetry.logs.max_queue_size",
        10_000,
        1,
        NodeScope
    );

    /**
     * PEM-encoded CA certificate(s) used to verify the otel-delivery-gateway's server certificate.
     * Multiple files are concatenated in PEM order. Paths are resolved relative to the Elasticsearch
     * config directory when not absolute. When empty, the OTel exporter's default TLS handling is used.
     */
    public static final Setting<List<String>> TELEMETRY_LOGS_SSL_CERTIFICATE_AUTHORITIES = Setting.stringListSetting(
        "telemetry.logs.ssl.certificate_authorities",
        NodeScope
    );

    /**
     * Path to the PEM-encoded client certificate for mTLS authentication to the otel-delivery-gateway.
     * Must be set together with {@link #TELEMETRY_LOGS_SSL_KEY}.
     * Path is resolved relative to the Elasticsearch config directory when not absolute.
     */
    public static final Setting<String> TELEMETRY_LOGS_SSL_CERTIFICATE = Setting.simpleString(
        "telemetry.logs.ssl.certificate",
        "",
        new Setting.Validator<>() {
            @Override
            public void validate(String value) {}

            @Override
            public void validate(String value, Map<Setting<?>, Object> settings) {
                String key = (String) settings.get(TELEMETRY_LOGS_SSL_KEY);
                if (value.isEmpty() != key.isEmpty()) {
                    throw new IllegalArgumentException(
                        TELEMETRY_LOGS_SSL_CERTIFICATE.getKey()
                            + " and "
                            + TELEMETRY_LOGS_SSL_KEY.getKey()
                            + " must both be set or both be unset"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(TELEMETRY_LOGS_SSL_KEY).iterator();
            }
        },
        NodeScope
    );

    /**
     * Path to the PEM-encoded private key for the client certificate ({@link #TELEMETRY_LOGS_SSL_CERTIFICATE}).
     * Must be set together with {@link #TELEMETRY_LOGS_SSL_CERTIFICATE}.
     * Supports PKCS#8 ({@code BEGIN PRIVATE KEY}) and PKCS#1 RSA ({@code BEGIN RSA PRIVATE KEY}) formats.
     * Encrypted keys are not supported.
     */
    public static final Setting<String> TELEMETRY_LOGS_SSL_KEY = Setting.simpleString(
        "telemetry.logs.ssl.key",
        "",
        new Setting.Validator<>() {
            @Override
            public void validate(String value) {}

            @Override
            public void validate(String value, Map<Setting<?>, Object> settings) {
                String cert = (String) settings.get(TELEMETRY_LOGS_SSL_CERTIFICATE);
                if (value.isEmpty() != cert.isEmpty()) {
                    throw new IllegalArgumentException(
                        TELEMETRY_LOGS_SSL_CERTIFICATE.getKey()
                            + " and "
                            + TELEMETRY_LOGS_SSL_KEY.getKey()
                            + " must both be set or both be unset"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(TELEMETRY_LOGS_SSL_CERTIFICATE).iterator();
            }
        },
        NodeScope
    );

    private static final class GreaterThanTimeValueValidator implements Setting.Validator<TimeValue> {

        private final String thisSettingKey;
        private final Setting<TimeValue> otherSetting;

        private GreaterThanTimeValueValidator(String thisSettingKey, Setting<TimeValue> otherSetting) {
            this.thisSettingKey = thisSettingKey;
            this.otherSetting = otherSetting;
        }

        @Override
        public void validate(TimeValue value) {}

        @Override
        public void validate(TimeValue value, Map<Setting<?>, Object> settings) {
            var otherValue = (TimeValue) settings.get(otherSetting);
            if (value.compareTo(otherValue) <= 0) {
                throw new IllegalArgumentException(
                    thisSettingKey + " (" + value + ") must be greater than " + otherSetting.getKey() + " (" + otherValue + ")"
                );
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return List.<Setting<?>>of(otherSetting).iterator();
        }
    }
}
