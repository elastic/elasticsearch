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
import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.common.settings.Setting.Property.OperatorDynamic;

/**
 * Node settings for the OpenTelemetry SDK metrics ({@link OtelSdkExportMeterSupplier}) and traces
 * ({@link OtelSdkExportTracerSupplier}) export paths.
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

    /** OTLP HTTP endpoint URL where the SDK exports buffered spans. Required when the SDK trace path is active. */
    public static final Setting<String> TELEMETRY_OTEL_TRACES_ENDPOINT = Setting.simpleString(
        "telemetry.otel.traces.endpoint",
        "",
        NodeScope
    );

    /** How often {@code BatchSpanProcessor} flushes buffered spans to the exporter. */
    public static final Setting<TimeValue> TELEMETRY_OTEL_TRACES_INTERVAL = Setting.timeSetting(
        "telemetry.otel.traces.interval",
        // 5 s matches the OTel SDK's default OTEL_BSP_SCHEDULE_DELAY. This caps span export latency
        // for under-full batches; full batches (default 512 spans) are flushed immediately regardless.
        TimeValue.timeValueSeconds(5),
        NodeScope
    );

    /**
     * Maximum local depth of exported spans, measured within this JVM. {@code 0} means only root spans
     * are exported (no children). Depth is computed from the local parent chain, not from remote
     * traceparent ancestors.
     */
    public static final Setting<Integer> TELEMETRY_OTEL_TRACES_MAX_TRACE_DEPTH = Setting.intSetting(
        "telemetry.otel.traces.max_trace_depth",
        0,
        0,
        OperatorDynamic,
        NodeScope
    );

    /** Best-effort upper bound on time spent flushing buffered metrics or spans to the exporter. */
    public static final Setting<TimeValue> TELEMETRY_OTEL_FLUSH_TIMEOUT = Setting.timeSetting(
        "telemetry.otel.flush_timeout",
        TimeValue.timeValueSeconds(10),
        NodeScope
    );

}
