/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.resource.v1.Resource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OtlpUtils {

    public static KeyValueList keyValueList(KeyValue... values) {
        return KeyValueList.newBuilder().addAllValues(List.of(values)).build();
    }

    public static KeyValue keyValue(String key, String value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setStringValue(value).build()).build();
    }

    public static KeyValue keyValue(String key, String... values) {
        return KeyValue.newBuilder()
            .setKey(key)
            .setValue(
                AnyValue.newBuilder()
                    .setArrayValue(
                        ArrayValue.newBuilder()
                            .addAllValues(Arrays.stream(values).map(v -> AnyValue.newBuilder().setStringValue(v).build()).toList())
                            .build()
                    )
                    .build()
            )
            .build();
    }

    public static KeyValue keyValue(String key, long value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setIntValue(value).build()).build();
    }

    public static KeyValue keyValue(String key, double value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setDoubleValue(value).build()).build();
    }

    public static KeyValue keyValue(String key, boolean value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setBoolValue(value).build()).build();
    }

    public static KeyValue keyValue(String key, KeyValueList keyValueList) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setKvlistValue(keyValueList).build()).build();
    }

    private static Resource createResource(List<KeyValue> attributes) {
        return Resource.newBuilder().addAllAttributes(attributes).build();
    }

    public static ResourceMetrics createResourceMetrics(List<KeyValue> attributes, List<ScopeMetrics> scopeMetrics) {
        return ResourceMetrics.newBuilder().setResource(createResource(attributes)).addAllScopeMetrics(scopeMetrics).build();
    }

    private static InstrumentationScope createScope(String name, String version) {
        return InstrumentationScope.newBuilder().setName(name).setVersion(version).build();
    }

    public static ScopeMetrics createScopeMetrics(String name, String version, Iterable<Metric> metrics) {
        return ScopeMetrics.newBuilder().setScope(createScope(name, version)).addAllMetrics(metrics).build();
    }

    public static Metric createGaugeMetric(String name, String unit, List<NumberDataPoint> dataPoints) {
        return Metric.newBuilder().setName(name).setUnit(unit).setGauge(Gauge.newBuilder().addAllDataPoints(dataPoints).build()).build();
    }

    public static Metric createSumMetric(
        String name,
        String unit,
        List<NumberDataPoint> dataPoints,
        boolean isMonotonic,
        AggregationTemporality temporality
    ) {
        return Metric.newBuilder()
            .setName(name)
            .setUnit(unit)
            .setSum(
                Sum.newBuilder().addAllDataPoints(dataPoints).setIsMonotonic(isMonotonic).setAggregationTemporality(temporality).build()
            )
            .build();
    }

    public static NumberDataPoint createDoubleDataPoint(long timestamp, List<KeyValue> attributes, double value) {
        return NumberDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(timestamp)
            .addAllAttributes(attributes)
            .setAsDouble(value)
            .build();
    }

    public static NumberDataPoint createLongDataPoint(long timestamp, List<KeyValue> attributes, long value) {
        return NumberDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(timestamp)
            .addAllAttributes(attributes)
            .setAsInt(value)
            .build();
    }

    public static ExportMetricsServiceRequest createMetricsRequest(List<Metric> metrics) {

        List<ResourceMetrics> resourceMetrics = new ArrayList<>();
        for (Metric metric : metrics) {
            resourceMetrics.add(
                createResourceMetrics(
                    List.of(keyValue("service.name", "test-service")),
                    List.of(createScopeMetrics("test", "1.0.0", List.of(metric)))
                )
            );
        }

        return ExportMetricsServiceRequest.newBuilder().addAllResourceMetrics(resourceMetrics).build();
    }
}
