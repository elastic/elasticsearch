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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.common.settings.Setting.Property.OperatorDynamic;

/**
 * Node settings for the OpenTelemetry SDK metrics ({@link OtelSdkExportMeterSupplier}) and traces
 * ({@link OtelSdkExportTracerSupplier}) export paths.
 */
public final class OtelSdkSettings {

    private OtelSdkSettings() {}

    /**
     * Worst-case time for one export through the OTLP retry chain: {@code max_attempts} request
     * timeouts plus the geometric backoffs between them. Used as the budget for flush, replay
     * joins, and executor termination.
     */
    static TimeValue computeExportOperationTimeout(Settings settings) {
        int maxAttempts = TELEMETRY_OTEL_METRICS_RETRY_MAX_ATTEMPTS.get(settings);
        long requestTimeoutMs = TELEMETRY_OTEL_METRICS_OTLP_REQUEST_TIMEOUT.get(settings).millis();
        long initialBackoffMs = TELEMETRY_OTEL_METRICS_RETRY_INITIAL_BACKOFF.get(settings).millis();
        double multiplier = TELEMETRY_OTEL_METRICS_RETRY_BACKOFF_MULTIPLIER.get(settings);

        double backoffSumMs = 0;
        double currentBackoffMs = initialBackoffMs;
        for (int i = 0; i < maxAttempts - 1; i++) {
            backoffSumMs += currentBackoffMs;
            currentBackoffMs *= multiplier;
        }
        return TimeValue.timeValueMillis((long) (maxAttempts * (double) requestTimeoutMs + backoffSumMs));
    }

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
        NodeScope
    );

    /** Buffered entries older than this are dropped. */
    public static final Setting<TimeValue> TELEMETRY_OTEL_METRICS_BUFFER_TTL = Setting.timeSetting(
        "telemetry.otel.metrics.buffer_ttl",
        TimeValue.timeValueHours(12),
        NodeScope
    );

    /**
     * In-memory queue between reader and exporter. Stays near 0 as long as the worst-case export
     * (see {@link #computeExportOperationTimeout(Settings)}) is shorter than the collection interval.
     */
    public static final Setting<Integer> TELEMETRY_OTEL_METRICS_EXPORT_QUEUE_SIZE = Setting.intSetting(
        "telemetry.otel.metrics.export_queue_size",
        8,
        0,
        NodeScope
    );

    /** Total attempts per export (initial + retries). {@code 1} disables retry. */
    public static final Setting<Integer> TELEMETRY_OTEL_METRICS_RETRY_MAX_ATTEMPTS = Setting.intSetting(
        "telemetry.otel.metrics.retry.max_attempts",
        2,
        1,
        5,
        NodeScope
    );

    /** Initial backoff between retry attempts. */
    public static final Setting<TimeValue> TELEMETRY_OTEL_METRICS_RETRY_INITIAL_BACKOFF = Setting.timeSetting(
        "telemetry.otel.metrics.retry.initial_backoff",
        TimeValue.timeValueSeconds(1),
        NodeScope
    );

    /** Backoff multiplier applied after each failed attempt. */
    public static final Setting<Double> TELEMETRY_OTEL_METRICS_RETRY_BACKOFF_MULTIPLIER = Setting.doubleSetting(
        "telemetry.otel.metrics.retry.backoff_multiplier",
        1.5,
        1.0,
        NodeScope
    );

    public static final Setting<TimeValue> TELEMETRY_OTEL_METRICS_OTLP_REQUEST_TIMEOUT = Setting.timeSetting(
        "telemetry.otel.metrics.otlp.request_timeout",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueSeconds(60),
        NodeScope
    );

    public static final Setting<TimeValue> TELEMETRY_OTEL_METRICS_OTLP_CONNECT_TIMEOUT = Setting.timeSetting(
        "telemetry.otel.metrics.otlp.connect_timeout",
        TimeValue.timeValueSeconds(2),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueSeconds(10),
        NodeScope
    );
}
