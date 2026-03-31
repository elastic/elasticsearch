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
import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;

import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomLong;

public class OtlpUtils {

    public static KeyValueList keyValueList(KeyValue... values) {
        return KeyValueList.newBuilder().addAllValues(List.of(values)).build();
    }

    public static KeyValue keyValue(String key, String value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setStringValue(value).build()).build();
    }

    public static List<KeyValue> mappingHints(String... mappingHints) {
        return List.of(keyValue(MappingHints.MAPPING_HINTS, mappingHints));
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

    public static Metric createExponentialHistogramMetric(
        String name,
        String unit,
        List<ExponentialHistogramDataPoint> dataPoints,
        AggregationTemporality temporality
    ) {
        return Metric.newBuilder()
            .setName(name)
            .setUnit(unit)
            .setExponentialHistogram(
                ExponentialHistogram.newBuilder().setAggregationTemporality(temporality).addAllDataPoints(dataPoints).build()
            )
            .build();
    }

    public static Metric createHistogramMetric(
        String name,
        String unit,
        List<HistogramDataPoint> dataPoints,
        AggregationTemporality temporality
    ) {
        return Metric.newBuilder()
            .setName(name)
            .setUnit(unit)
            .setHistogram(Histogram.newBuilder().setAggregationTemporality(temporality).addAllDataPoints(dataPoints).build())
            .build();
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

    public static Metric createSummaryMetric(String name, String unit, List<SummaryDataPoint> dataPoints) {
        return Metric.newBuilder()
            .setName(name)
            .setUnit(unit)
            .setSummary(Summary.newBuilder().addAllDataPoints(dataPoints).build())
            .build();
    }

    public static NumberDataPoint createDoubleDataPoint(long timestamp) {
        return createDoubleDataPoint(timestamp, timestamp, List.of());
    }

    public static NumberDataPoint createDoubleDataPoint(long timeUnixNano, long startTimeUnixNano, List<KeyValue> attributes) {
        return NumberDataPoint.newBuilder()
            .setTimeUnixNano(timeUnixNano)
            .setStartTimeUnixNano(startTimeUnixNano)
            .addAllAttributes(attributes)
            .setAsDouble(randomDouble())
            .build();
    }

    public static NumberDataPoint createLongDataPoint(long timestamp) {
        return createLongDataPoint(timestamp, timestamp, List.of());
    }

    public static NumberDataPoint createLongDataPoint(long timeUnixNano, long startTimeUnixNano, List<KeyValue> attributes) {
        return NumberDataPoint.newBuilder()
            .setTimeUnixNano(timeUnixNano)
            .setStartTimeUnixNano(startTimeUnixNano)
            .addAllAttributes(attributes)
            .setAsInt(randomLong())
            .build();
    }

    public static SummaryDataPoint createSummaryDataPoint(long timestamp, List<KeyValue> attributes) {
        return SummaryDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(timestamp)
            .addAllAttributes(attributes)
            .setCount(randomLong())
            .setSum(randomDouble())
            .build();
    }

    public static ExportMetricsServiceRequest createMetricsRequest(List<Metric> metrics) {
        return createMetricsRequest(List.of(keyValue("service.name", "test-service")), metrics);
    }

    public static ExportMetricsServiceRequest createMetricsRequest(List<KeyValue> resourceAttributes, List<Metric> metrics) {
        List<ResourceMetrics> resourceMetrics = new ArrayList<>();
        for (Metric metric : metrics) {
            resourceMetrics.add(createResourceMetrics(resourceAttributes, List.of(createScopeMetrics("test", "1.0.0", List.of(metric)))));
        }

        return ExportMetricsServiceRequest.newBuilder().addAllResourceMetrics(resourceMetrics).build();
    }
}
