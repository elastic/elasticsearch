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
     * Default aggregation applied to every {@code HISTOGRAM} instrument exported by the OTel SDK.
     * <p>
     * Histograms emitted by Elasticsearch land in metric indices whose ES field type is determined by the
     * OTLP wire shape. With {@link HistogramAggregation#EXPLICIT_BUCKET_HISTOGRAM} (the default, matching
     * OTel's built-in default and the behavior prior to this setting being introduced) histograms map to
     * the ES {@code histogram} field type, which is <strong>not</strong> queryable by ES|QL
     * {@code percentile()}. With {@link HistogramAggregation#BASE2_EXPONENTIAL_BUCKET_HISTOGRAM} they map
     * to {@code exponential_histogram}, which ES|QL {@code percentile()} accepts.
     * <p>
     * This is set programmatically (via {@link OtelSdkExportMeterSupplier}) rather than read from the
     * OTel autoconfigure system property {@code otel.exporter.otlp.metrics.default.histogram.aggregation}
     * because the OTLP exporter here is hand-built and bypasses autoconfigure.
     * <p>
     * Note: when the elastic-apm-java agent is also loaded into the JVM (e.g. with
     * {@code telemetry.agent.server_url} configured for tracing) its parallel metrics exporter does not
     * yet handle exponential histograms and will throw {@code IndexOutOfBoundsException} per export
     * cycle. Disable the offending instrumentation with
     * {@code -Delastic.apm.disable_instrumentations=otelmetricsdk} until
     * <a href="https://github.com/elastic/apm-agent-java/issues/4464">elastic/apm-agent-java#4464</a>
     * is resolved.
     */
    public static final Setting<HistogramAggregation> TELEMETRY_OTEL_METRICS_HISTOGRAM_AGGREGATION = Setting.enumSetting(
        HistogramAggregation.class,
        "telemetry.otel.metrics.histogram.aggregation",
        HistogramAggregation.EXPLICIT_BUCKET_HISTOGRAM,
        NodeScope
    );

    public enum HistogramAggregation {
        EXPLICIT_BUCKET_HISTOGRAM,
        BASE2_EXPONENTIAL_BUCKET_HISTOGRAM
    }
}
