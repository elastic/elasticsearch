/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.GaugeData;
import io.opentelemetry.sdk.metrics.data.HistogramData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.data.SumData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.core.Types;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeMapValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

/**
 * Serializes and deserializes OTel SDK {@link MetricData} using XContent JSON.
 */
final class MetricDataSerializer {

    private MetricDataSerializer() {}

    static void serialize(Collection<MetricData> metrics, OutputStream out) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder(out)) {
            builder.startArray();
            for (MetricData m : metrics) {
                builder.map(toMap(m));
            }
            builder.endArray();
        }
    }

    static List<MetricData> deserialize(InputStream in) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, in)) {
            if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                throw new IOException("expected JSON array");
            }
            List<MetricData> result = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                result.add(fromMap(parser.map()));
            }
            return result;
        }
    }

    private static Map<String, Object> toMap(MetricData m) throws IOException {
        var map = new LinkedHashMap<String, Object>();
        map.put("name", m.getName());
        map.put("description", m.getDescription());
        map.put("unit", m.getUnit());
        map.put("type", m.getType().name());
        map.put("resource", attrEntries(m.getResource().getAttributes()));
        map.put("scope", scopeToMap(m.getInstrumentationScopeInfo()));

        switch (m.getType()) {
            case LONG_GAUGE -> map.put(
                "points",
                mapPoints(m.getLongGaugeData().getPoints(), (pm, p) -> pm.put("long_value", p.getValue()))
            );
            case DOUBLE_GAUGE -> map.put(
                "points",
                mapPoints(m.getDoubleGaugeData().getPoints(), (pm, p) -> pm.put("double_value", p.getValue()))
            );
            case LONG_SUM -> {
                SumData<LongPointData> sum = m.getLongSumData();
                map.put("is_monotonic", sum.isMonotonic());
                map.put("temporality", sum.getAggregationTemporality().name());
                map.put("points", mapPoints(sum.getPoints(), (pm, p) -> pm.put("long_value", p.getValue())));
            }
            case DOUBLE_SUM -> {
                SumData<DoublePointData> sum = m.getDoubleSumData();
                map.put("is_monotonic", sum.isMonotonic());
                map.put("temporality", sum.getAggregationTemporality().name());
                map.put("points", mapPoints(sum.getPoints(), (pm, p) -> pm.put("double_value", p.getValue())));
            }
            case HISTOGRAM -> {
                HistogramData hist = m.getHistogramData();
                map.put("temporality", hist.getAggregationTemporality().name());
                map.put("points", mapPoints(hist.getPoints(), (pm, p) -> {
                    pm.put("sum", p.getSum());
                    pm.put("count", p.getCount());
                    pm.put("has_min", p.hasMin());
                    pm.put("min", p.getMin());
                    pm.put("has_max", p.hasMax());
                    pm.put("max", p.getMax());
                    pm.put("boundaries", p.getBoundaries());
                    pm.put("counts", p.getCounts());
                }));
            }
            default -> throw new IOException("unsupported metric type: " + m.getType());
        }
        return map;
    }

    private static Map<String, Object> scopeToMap(InstrumentationScopeInfo scope) {
        var map = new LinkedHashMap<String, Object>();
        map.put("name", scope.getName());
        if (scope.getVersion() != null) map.put("version", scope.getVersion());
        if (scope.getSchemaUrl() != null) map.put("schema_url", scope.getSchemaUrl());
        return map;
    }

    private static <T extends PointData> List<Map<String, Object>> mapPoints(
        Collection<T> points,
        BiConsumer<Map<String, Object>, T> extra
    ) {
        return points.stream().map(point -> {
            Map<String, Object> pm = pointBase(point);
            extra.accept(pm, point);
            return pm;
        }).toList();
    }

    private static Map<String, Object> pointBase(PointData p) {
        var map = new LinkedHashMap<String, Object>();
        map.put("start_epoch_nanos", p.getStartEpochNanos());
        map.put("epoch_nanos", p.getEpochNanos());
        map.put("attributes", attrEntries(p.getAttributes()));
        return map;
    }

    private static List<Map<String, Object>> attrEntries(Attributes attrs) {
        return attrs.asMap()
            .entrySet()
            .stream()
            .map(e -> Map.of("key", e.getKey().getKey(), "type", e.getKey().getType().name(), "value", e.getValue()))
            .toList();
    }

    private static MetricData fromMap(Map<String, Object> map) throws IOException {
        Resource resource = readResource(Types.forciblyCast(map.get("resource")));
        InstrumentationScopeInfo scope = readScope(nodeMapValue(map.get("scope"), "scope"));
        String name = nodeStringValue(map.get("name"));
        String description = nodeStringValue(map.get("description"));
        String unit = nodeStringValue(map.get("unit"));
        MetricDataType type = MetricDataType.valueOf(nodeStringValue(map.get("type")));
        List<Map<String, Object>> points = Types.forciblyCast(map.get("points"));

        Data<? extends PointData> data = switch (type) {
            case LONG_GAUGE -> GaugeData.createLongGaugeData(readLongPoints(points));
            case DOUBLE_GAUGE -> GaugeData.createDoubleGaugeData(readDoublePoints(points));
            case LONG_SUM -> SumData.createLongSumData(
                nodeBooleanValue(map.get("is_monotonic")),
                AggregationTemporality.valueOf(nodeStringValue(map.get("temporality"))),
                readLongPoints(points)
            );
            case DOUBLE_SUM -> SumData.createDoubleSumData(
                nodeBooleanValue(map.get("is_monotonic")),
                AggregationTemporality.valueOf(nodeStringValue(map.get("temporality"))),
                readDoublePoints(points)
            );
            case HISTOGRAM -> HistogramData.create(
                AggregationTemporality.valueOf(nodeStringValue(map.get("temporality"))),
                readHistogramPoints(points)
            );
            default -> throw new IOException("unsupported metric type: " + type);
        };

        return new SerializedMetricData(resource, scope, name, description, unit, type, data);
    }

    private static Resource readResource(List<Map<String, Object>> entries) {
        if (entries == null || entries.isEmpty()) {
            return Resource.getDefault();
        }
        return Resource.create(readAttributes(entries));
    }

    private static InstrumentationScopeInfo readScope(Map<String, Object> map) {
        String name = nodeStringValue(map.get("name"));
        String version = nodeStringValue(map.get("version"));
        String schemaUrl = nodeStringValue(map.get("schema_url"));
        if (version == null && schemaUrl == null) {
            return InstrumentationScopeInfo.create(name);
        }
        var builder = InstrumentationScopeInfo.builder(name);
        if (version != null) builder.setVersion(version);
        if (schemaUrl != null) builder.setSchemaUrl(schemaUrl);
        return builder.build();
    }

    private static List<LongPointData> readLongPoints(List<Map<String, Object>> points) {
        return points.stream()
            .map(
                p -> LongPointData.create(
                    nodeLongValue(p.get("start_epoch_nanos")),
                    nodeLongValue(p.get("epoch_nanos")),
                    readAttributes(Types.forciblyCast(p.get("attributes"))),
                    nodeLongValue(p.get("long_value"))
                )
            )
            .toList();
    }

    private static List<DoublePointData> readDoublePoints(List<Map<String, Object>> points) {
        return points.stream()
            .map(
                p -> DoublePointData.create(
                    nodeLongValue(p.get("start_epoch_nanos")),
                    nodeLongValue(p.get("epoch_nanos")),
                    readAttributes(Types.forciblyCast(p.get("attributes"))),
                    nodeDoubleValue(p.get("double_value")),
                    Collections.emptyList()
                )
            )
            .toList();
    }

    private static List<HistogramPointData> readHistogramPoints(List<Map<String, Object>> points) {
        List<HistogramPointData> result = new ArrayList<>(points.size());
        for (Map<String, Object> p : points) {
            List<Number> rawBoundaries = Types.forciblyCast(p.get("boundaries"));
            List<Number> rawCounts = Types.forciblyCast(p.get("counts"));
            result.add(
                HistogramPointData.create(
                    nodeLongValue(p.get("start_epoch_nanos")),
                    nodeLongValue(p.get("epoch_nanos")),
                    readAttributes(Types.forciblyCast(p.get("attributes"))),
                    nodeDoubleValue(p.get("sum")),
                    nodeBooleanValue(p.get("has_min")),
                    nodeDoubleValue(p.get("min")),
                    nodeBooleanValue(p.get("has_max")),
                    nodeDoubleValue(p.get("max")),
                    rawBoundaries.stream().map(Number::doubleValue).toList(),
                    rawCounts.stream().map(Number::longValue).toList()
                )
            );
        }
        return result;
    }

    private static Attributes readAttributes(List<Map<String, Object>> entries) {
        if (entries == null || entries.isEmpty()) return Attributes.empty();
        AttributesBuilder builder = Attributes.builder();
        for (Map<String, Object> entry : entries) {
            String key = nodeStringValue(entry.get("key"));
            AttributeType attrType = AttributeType.valueOf(nodeStringValue(entry.get("type")));
            Object value = entry.get("value");
            switch (attrType) {
                case STRING -> builder.put(AttributeKey.stringKey(key), (String) value);
                case BOOLEAN -> builder.put(AttributeKey.booleanKey(key), (Boolean) value);
                case LONG -> builder.put(AttributeKey.longKey(key), ((Number) value).longValue());
                case DOUBLE -> builder.put(AttributeKey.doubleKey(key), ((Number) value).doubleValue());
                case STRING_ARRAY -> {
                    List<String> strings = Types.forciblyCast(entry.get("value"));
                    builder.put(AttributeKey.stringArrayKey(key), strings);
                }
                case BOOLEAN_ARRAY -> {
                    List<Boolean> bools = Types.forciblyCast(entry.get("value"));
                    builder.put(AttributeKey.booleanArrayKey(key), bools);
                }
                case LONG_ARRAY -> {
                    List<Number> nums = Types.forciblyCast(entry.get("value"));
                    builder.put(AttributeKey.longArrayKey(key), nums.stream().map(Number::longValue).toList());
                }
                case DOUBLE_ARRAY -> {
                    List<Number> nums = Types.forciblyCast(entry.get("value"));
                    builder.put(AttributeKey.doubleArrayKey(key), nums.stream().map(Number::doubleValue).toList());
                }
            }
        }
        return builder.build();
    }

    private record SerializedMetricData(
        Resource resource,
        InstrumentationScopeInfo scope,
        String name,
        String description,
        String unit,
        MetricDataType type,
        Data<? extends PointData> data
    ) implements MetricData {
        @Override
        public Resource getResource() {
            return resource;
        }

        @Override
        public InstrumentationScopeInfo getInstrumentationScopeInfo() {
            return scope;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public String getUnit() {
            return unit;
        }

        @Override
        public MetricDataType getType() {
            return type;
        }

        @Override
        public Data<? extends PointData> getData() {
            return data;
        }
    }
}
