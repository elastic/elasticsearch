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

import static org.elasticsearch.common.settings.Setting.Property.NodeScope;

/**
 * Node settings for OpenTelemetry SDK metrics export ({@link OtelSdkExportMeterSupplier}).
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

    /**
     * Maximum disk space for buffered metric batches when the OTLP endpoint is unreachable.
     * Set to {@code 0b} to disable disk buffering entirely (pass-through mode).
     */
    public static final Setting<ByteSizeValue> TELEMETRY_OTEL_METRICS_DISK_BUFFER_SIZE = Setting.byteSizeSetting(
        "telemetry.otel.metrics.disk_buffer_size",
        ByteSizeValue.ZERO,
        NodeScope
    );

    /**
     * Maximum age for buffered metric data. Entries older than this are dropped without export.
     */
    public static final Setting<TimeValue> TELEMETRY_OTEL_METRICS_BUFFER_TTL = Setting.timeSetting(
        "telemetry.otel.metrics.buffer_ttl",
        TimeValue.timeValueMinutes(5),
        NodeScope
    );

    // -- OTLP exporter retry policy --

    /**
     * Maximum number of export attempts (including the initial request).
     * Valid range is 1–5; set to {@code 1} to disable retry.
     */
    public static final Setting<Integer> TELEMETRY_OTEL_METRICS_RETRY_MAX_ATTEMPTS = Setting.intSetting(
        "telemetry.otel.metrics.retry.max_attempts",
        5,
        1,
        5,
        NodeScope
    );

    /**
     * Initial backoff duration between retry attempts.
     */
    public static final Setting<TimeValue> TELEMETRY_OTEL_METRICS_RETRY_INITIAL_BACKOFF = Setting.timeSetting(
        "telemetry.otel.metrics.retry.initial_backoff",
        TimeValue.timeValueSeconds(1),
        NodeScope
    );

    /**
     * Maximum backoff duration between retry attempts.
     */
    public static final Setting<TimeValue> TELEMETRY_OTEL_METRICS_RETRY_MAX_BACKOFF = Setting.timeSetting(
        "telemetry.otel.metrics.retry.max_backoff",
        TimeValue.timeValueSeconds(5),
        NodeScope
    );

    /**
     * Multiplier applied to the backoff duration after each failed attempt.
     */
    public static final Setting<Double> TELEMETRY_OTEL_METRICS_RETRY_BACKOFF_MULTIPLIER = Setting.doubleSetting(
        "telemetry.otel.metrics.retry.backoff_multiplier",
        1.5,
        1.0,
        NodeScope
    );
}
