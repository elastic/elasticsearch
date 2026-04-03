/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses OTLP protobuf metrics and produces protocol-neutral {@link ReceivedTelemetry} so that
 * tests can assert in a format-independent way. Builds APM-shaped JSON per data point and
 * delegates to {@link ApmIntakeMessageParser} for the JSON to ADT step.
 */
public final class OtlpMetricsParser {

    private OtlpMetricsParser() {}

    /**
     * Parse an OTLP metrics request into a list of received telemetry events.
     *
     * @param input OTLP protobuf ExportMetricsServiceRequest stream
     * @return list of ReceivedTelemetry (one per metric data point)
     * @throws IOException if the stream is not valid OTLP protobuf
     */
    public static List<ReceivedTelemetry> parse(InputStream input) throws IOException {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.parseFrom(input);
        List<ReceivedTelemetry> result = new ArrayList<>();
        for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
            for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                String scopeName = scopeMetrics.getScope().getName();
                for (Metric metric : scopeMetrics.getMetricsList()) {
                    switch (metric.getDataCase()) {
                        case SUM, GAUGE -> {
                            var dataPoints = metric.getDataCase() == Metric.DataCase.SUM
                                ? metric.getSum().getDataPointsList()
                                : metric.getGauge().getDataPointsList();
                            for (NumberDataPoint dp : dataPoints) {
                                var builder = XContentFactory.jsonBuilder().startObject().startObject("metricset");
                                writeTags(builder, scopeName, dp.getAttributesList());
                                builder.startObject("samples").startObject(metric.getName());
                                switch (dp.getValueCase()) {
                                    case AS_DOUBLE -> builder.field("value", dp.getAsDouble());
                                    case AS_INT -> builder.field("value", dp.getAsInt());
                                }
                                builder.endObject().endObject();
                                String jsonLine = Strings.toString(builder.endObject().endObject());
                                ApmIntakeMessageParser.parseLine(jsonLine).ifPresent(result::add);
                            }
                        }
                        case HISTOGRAM -> {
                            for (HistogramDataPoint dp : metric.getHistogram().getDataPointsList()) {
                                var builder = XContentFactory.jsonBuilder().startObject().startObject("metricset");
                                writeTags(builder, scopeName, dp.getAttributesList());
                                builder.startObject("samples").startObject(metric.getName());
                                builder.field("counts", dp.getBucketCountsList());
                                builder.endObject().endObject();
                                String jsonLine = Strings.toString(builder.endObject().endObject());
                                ApmIntakeMessageParser.parseLine(jsonLine).ifPresent(result::add);
                            }
                        }
                        default -> {
                            var builder = XContentFactory.jsonBuilder().startObject().startObject("metricset");
                            writeTags(builder, scopeName, List.of());
                            builder.startObject("samples").startObject(metric.getName()).endObject().endObject();
                            String jsonLine = Strings.toString(builder.endObject().endObject());
                            ApmIntakeMessageParser.parseLine(jsonLine).ifPresent(result::add);
                        }
                    }
                }
            }
        }
        return result;
    }

    private static void writeTags(XContentBuilder builder, String scopeName, List<KeyValue> attributes) throws IOException {
        builder.startObject("tags");
        builder.field("otel_instrumentation_scope_name", scopeName);
        for (KeyValue kv : attributes) {
            switch (kv.getValue().getValueCase()) {
                case STRING_VALUE -> builder.field(kv.getKey(), kv.getValue().getStringValue());
                case INT_VALUE -> builder.field(kv.getKey(), kv.getValue().getIntValue());
                case DOUBLE_VALUE -> builder.field(kv.getKey(), kv.getValue().getDoubleValue());
                case BOOL_VALUE -> builder.field(kv.getKey(), kv.getValue().getBoolValue());
                default -> {
                }
            }
        }
        builder.endObject();
    }
}
