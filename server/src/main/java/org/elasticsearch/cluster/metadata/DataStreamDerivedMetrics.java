/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Describes write-path metrics that Elasticsearch derives from documents written to a data stream.
 */
public record DataStreamDerivedMetrics(
    boolean enabled,
    List<String> builtin,
    List<TimeValue> intervals,
    List<String> dimensions,
    List<Metric> metrics
) implements Writeable, ToXContentObject {

    public static final ParseField ENABLED_FIELD = new ParseField("enabled");
    public static final ParseField BUILTIN_FIELD = new ParseField("builtin");
    public static final ParseField INTERVALS_FIELD = new ParseField("intervals");
    public static final ParseField DIMENSIONS_FIELD = new ParseField("dimensions");
    public static final ParseField METRICS_FIELD = new ParseField("metrics");
    public static final TimeValue DEFAULT_INTERVAL = TimeValue.timeValueSeconds(10);
    public static final List<String> DEFAULT_BUILTIN = List.of("ingest.*");

    private static final int MAX_INTERVALS = 8;
    private static final int MAX_DIMENSIONS = 16;
    private static final int MAX_METRIC_DIMENSIONS = 16;
    private static final int MAX_USER_METRICS = 64;
    private static final TimeValue MIN_INTERVAL = TimeValue.timeValueSeconds(1);
    private static final Set<String> ALLOWED_BUILTIN = Set.of(
        "ingest.*",
        "ingest.docs.count",
        "ingest.docs.rate",
        "ingest.bytes.count",
        "ingest.bytes.rate",
        "ingest.failures.count",
        "ingest.failures.rate"
    );

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DataStreamDerivedMetrics, Void> PARSER = new ConstructingObjectParser<>(
        "derived_metrics",
        false,
        args -> fromTemplate(
            new Template(
                (Boolean) args[0],
                (List<String>) args[1],
                parseIntervals((List<String>) args[2]),
                (List<String>) args[3],
                (List<Metric>) args[4]
            )
        )
    );

    static {
        PARSER.declareBoolean(optionalConstructorArg(), ENABLED_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), BUILTIN_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), INTERVALS_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), DIMENSIONS_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> Metric.fromXContent(p), METRICS_FIELD);
    }

    public DataStreamDerivedMetrics {
        builtin = builtin == null ? DEFAULT_BUILTIN : List.copyOf(builtin);
        intervals = intervals == null ? List.of(DEFAULT_INTERVAL) : List.copyOf(intervals);
        dimensions = dimensions == null ? List.of() : List.copyOf(dimensions);
        metrics = metrics == null ? List.of() : List.copyOf(metrics);
        validateBuiltin(builtin);
        validateIntervals(intervals);
        validateDimensions(dimensions, MAX_DIMENSIONS, DIMENSIONS_FIELD.getPreferredName());
        validateMetrics(metrics);
    }

    public DataStreamDerivedMetrics(StreamInput in) throws IOException {
        this(
            in.readBoolean(),
            in.readStringCollectionAsList(),
            in.readCollectionAsList(StreamInput::readTimeValue),
            in.readStringCollectionAsList(),
            in.readCollectionAsList(Metric::new)
        );
    }

    public static DataStreamDerivedMetrics read(StreamInput in) throws IOException {
        return new DataStreamDerivedMetrics(in);
    }

    public static DataStreamDerivedMetrics fromTemplate(Template template) {
        return new DataStreamDerivedMetrics(
            template.enabled == null ? true : template.enabled,
            template.builtin,
            template.intervals,
            template.dimensions,
            template.metrics
        );
    }

    public static DataStreamDerivedMetrics fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private static List<TimeValue> parseIntervals(@Nullable List<String> intervals) {
        if (intervals == null) {
            return null;
        }
        return intervals.stream().map(value -> TimeValue.parseTimeValue(value, INTERVALS_FIELD.getPreferredName())).toList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeStringCollection(builtin);
        out.writeCollection(intervals, StreamOutput::writeTimeValue);
        out.writeStringCollection(dimensions);
        out.writeCollection(metrics);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED_FIELD.getPreferredName(), enabled);
        builder.stringListField(BUILTIN_FIELD.getPreferredName(), builtin);
        builder.startArray(INTERVALS_FIELD.getPreferredName());
        for (TimeValue interval : intervals) {
            builder.value(interval.getStringRep());
        }
        builder.endArray();
        if (dimensions.isEmpty() == false) {
            builder.stringListField(DIMENSIONS_FIELD.getPreferredName(), dimensions);
        }
        if (metrics.isEmpty() == false) {
            builder.xContentList(METRICS_FIELD.getPreferredName(), metrics);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    private static void validateBuiltin(List<String> builtin) {
        for (String metric : builtin) {
            if (ALLOWED_BUILTIN.contains(metric) == false) {
                throw new IllegalArgumentException(
                    "unsupported derived metrics builtin [" + metric + "], supported values are " + ALLOWED_BUILTIN
                );
            }
        }
    }

    private static void validateIntervals(List<TimeValue> intervals) {
        if (intervals.isEmpty()) {
            throw new IllegalArgumentException("derived metrics requires at least one interval");
        }
        if (intervals.size() > MAX_INTERVALS) {
            throw new IllegalArgumentException("derived metrics supports at most [" + MAX_INTERVALS + "] intervals");
        }
        for (TimeValue interval : intervals) {
            if (interval.compareTo(MIN_INTERVAL) < 0) {
                throw new IllegalArgumentException("derived metrics interval [" + interval + "] must be at least [" + MIN_INTERVAL + "]");
            }
        }
    }

    private static void validateDimensions(List<String> dimensions, int maxDimensions, String fieldName) {
        if (dimensions.size() > maxDimensions) {
            throw new IllegalArgumentException("derived metrics [" + fieldName + "] supports at most [" + maxDimensions + "] dimensions");
        }
        for (String dimension : dimensions) {
            validateFieldName(dimension, fieldName);
            if (dimension.startsWith("derivative.") || dimension.startsWith("derived_metrics.")) {
                throw new IllegalArgumentException("derived metrics dimension [" + dimension + "] uses a reserved prefix");
            }
        }
    }

    private static void validateMetrics(List<Metric> metrics) {
        if (metrics.size() > MAX_USER_METRICS) {
            throw new IllegalArgumentException("derived metrics supports at most [" + MAX_USER_METRICS + "] user metrics");
        }
        Map<String, Metric> byName = new LinkedHashMap<>();
        for (Metric metric : metrics) {
            Metric existing = byName.putIfAbsent(metric.name(), metric);
            if (existing != null && existing.equals(metric) == false) {
                throw new IllegalArgumentException("derived metric [" + metric.name() + "] is defined more than once");
            }
        }
    }

    private static void validateFieldName(String field, String context) {
        if (Strings.hasText(field) == false) {
            throw new IllegalArgumentException("derived metrics [" + context + "] must not contain empty field names");
        }
    }

    public enum MetricType {
        COUNTER,
        GAUGE,
        HISTOGRAM;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        static MetricType fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    public enum GaugeAggregation {
        FIRST_VALUE,
        LAST_VALUE,
        MIN,
        MAX,
        AVG,
        SUM;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        static GaugeAggregation fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    public record Metric(
        String name,
        MetricType type,
        @Nullable Map<String, Object> when,
        MetricValue value,
        @Nullable GaugeAggregation aggregation,
        List<String> dimensions
    ) implements Writeable, ToXContentObject {

        public static final ParseField NAME_FIELD = new ParseField("name");
        public static final ParseField TYPE_FIELD = new ParseField("type");
        public static final ParseField WHEN_FIELD = new ParseField("when");
        public static final ParseField VALUE_FIELD = new ParseField("value");
        public static final ParseField AGGREGATION_FIELD = new ParseField("aggregation");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Metric, Void> PARSER = new ConstructingObjectParser<>(
            "derived_metric",
            false,
            args -> new Metric(
                (String) args[0],
                MetricType.fromString((String) args[1]),
                copyMap((Map<String, Object>) args[2]),
                (MetricValue) args[3],
                args[4] == null ? null : GaugeAggregation.fromString((String) args[4]),
                (List<String>) args[5]
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
            PARSER.declareField(optionalConstructorArg(), (p, c) -> p.mapOrdered(), WHEN_FIELD, ObjectParser.ValueType.OBJECT);
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> MetricValue.fromXContent(p),
                VALUE_FIELD,
                ObjectParser.ValueType.OBJECT_OR_NUMBER
            );
            PARSER.declareString(optionalConstructorArg(), AGGREGATION_FIELD);
            PARSER.declareStringArray(optionalConstructorArg(), DIMENSIONS_FIELD);
        }

        public Metric {
            Objects.requireNonNull(type, "derived metric type must not be null");
            if (Strings.hasText(name) == false) {
                throw new IllegalArgumentException("derived metric name must not be empty");
            }
            if (name.startsWith("ingest.")) {
                throw new IllegalArgumentException("derived metric name [" + name + "] uses reserved [ingest.*] namespace");
            }
            when = copyMap(when);
            value = value == null && type == MetricType.COUNTER ? MetricValue.constant(1.0) : value;
            if (value == null) {
                throw new IllegalArgumentException("derived metric [" + name + "] requires a value");
            }
            dimensions = dimensions == null ? List.of() : List.copyOf(dimensions);
            validatePredicate(when, "when");
            validateDimensions(dimensions, MAX_METRIC_DIMENSIONS, "metrics.dimensions");
            if (type == MetricType.GAUGE) {
                aggregation = aggregation == null ? GaugeAggregation.LAST_VALUE : aggregation;
            } else if (aggregation != null) {
                throw new IllegalArgumentException("derived metric [" + name + "] only supports [aggregation] for gauge metrics");
            }
        }

        public Metric(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readEnum(MetricType.class),
                copyMap(in.readGenericMap()),
                new MetricValue(in),
                in.readOptionalEnum(GaugeAggregation.class),
                in.readStringCollectionAsList()
            );
        }

        public static Metric fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeEnum(type);
            out.writeGenericMap(when);
            value.writeTo(out);
            out.writeOptionalEnum(aggregation);
            out.writeStringCollection(dimensions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME_FIELD.getPreferredName(), name);
            builder.field(TYPE_FIELD.getPreferredName(), type);
            if (when != null) {
                builder.field(WHEN_FIELD.getPreferredName(), when);
            }
            builder.field(VALUE_FIELD.getPreferredName(), value);
            if (aggregation != null) {
                builder.field(AGGREGATION_FIELD.getPreferredName(), aggregation);
            }
            if (dimensions.isEmpty() == false) {
                builder.stringListField(DIMENSIONS_FIELD.getPreferredName(), dimensions);
            }
            builder.endObject();
            return builder;
        }
    }

    public record MetricValue(@Nullable Double constant, @Nullable String field) implements Writeable, ToXContent {
        private static final ParseField FIELD_FIELD = new ParseField("field");

        public MetricValue {
            if ((constant == null) == (field == null)) {
                throw new IllegalArgumentException("derived metric value must be either a numeric constant or a field reference");
            }
            if (field != null) {
                validateFieldName(field, Metric.VALUE_FIELD.getPreferredName());
            }
        }

        public MetricValue(StreamInput in) throws IOException {
            this(in.readOptionalDouble(), in.readOptionalString());
        }

        public static MetricValue constant(double value) {
            return new MetricValue(value, null);
        }

        public static MetricValue field(String field) {
            return new MetricValue(null, field);
        }

        public static MetricValue fromXContent(XContentParser parser) throws IOException {
            return switch (parser.currentToken()) {
                case VALUE_NUMBER -> constant(parser.doubleValue());
                case START_OBJECT -> {
                    Map<String, Object> value = parser.map();
                    if (value.size() != 1 || value.get(FIELD_FIELD.getPreferredName()) instanceof String == false) {
                        throw new IllegalArgumentException("derived metric value object must contain only [field]");
                    }
                    yield field((String) value.get(FIELD_FIELD.getPreferredName()));
                }
                default -> throw new IllegalArgumentException("derived metric value must be a number or an object");
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalDouble(constant);
            out.writeOptionalString(field);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (constant != null) {
                return builder.value(constant);
            }
            builder.startObject();
            builder.field(FIELD_FIELD.getPreferredName(), field);
            builder.endObject();
            return builder;
        }
    }

    /**
     * Template form of {@link DataStreamDerivedMetrics}. Null fields mean the template does not define that property.
     */
    public record Template(
        @Nullable Boolean enabled,
        @Nullable List<String> builtin,
        @Nullable List<TimeValue> intervals,
        @Nullable List<String> dimensions,
        @Nullable List<Metric> metrics
    ) implements Writeable, ToXContentObject {

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Template, Void> PARSER = new ConstructingObjectParser<>(
            "derived_metrics_template",
            false,
            args -> new Template(
                (Boolean) args[0],
                (List<String>) args[1],
                parseIntervals((List<String>) args[2]),
                (List<String>) args[3],
                (List<Metric>) args[4]
            )
        );

        static {
            PARSER.declareBoolean(optionalConstructorArg(), ENABLED_FIELD);
            PARSER.declareStringArray(optionalConstructorArg(), BUILTIN_FIELD);
            PARSER.declareStringArray(optionalConstructorArg(), INTERVALS_FIELD);
            PARSER.declareStringArray(optionalConstructorArg(), DIMENSIONS_FIELD);
            PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> Metric.fromXContent(p), METRICS_FIELD);
        }

        public Template {
            builtin = builtin == null ? null : List.copyOf(builtin);
            intervals = intervals == null ? null : List.copyOf(intervals);
            dimensions = dimensions == null ? null : List.copyOf(dimensions);
            metrics = metrics == null ? null : List.copyOf(metrics);
            if (builtin != null) {
                validateBuiltin(builtin);
            }
            if (intervals != null) {
                validateIntervals(intervals);
            }
            if (dimensions != null) {
                validateDimensions(dimensions, MAX_DIMENSIONS, DIMENSIONS_FIELD.getPreferredName());
            }
            if (metrics != null) {
                validateMetrics(metrics);
            }
        }

        public Template(StreamInput in) throws IOException {
            this(
                in.readOptionalBoolean(),
                in.readOptionalStringCollectionAsList(),
                in.readOptionalCollectionAsList(StreamInput::readTimeValue),
                in.readOptionalStringCollectionAsList(),
                in.readOptionalCollectionAsList(Metric::new)
            );
        }

        public static Template read(StreamInput in) throws IOException {
            return new Template(in);
        }

        public static Template fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalBoolean(enabled);
            out.writeOptionalStringCollection(builtin);
            out.writeOptionalCollection(intervals, StreamOutput::writeTimeValue);
            out.writeOptionalStringCollection(dimensions);
            out.writeOptionalCollection(metrics, StreamOutput::writeWriteable);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (enabled != null) {
                builder.field(ENABLED_FIELD.getPreferredName(), enabled);
            }
            if (builtin != null) {
                builder.stringListField(BUILTIN_FIELD.getPreferredName(), builtin);
            }
            if (intervals != null) {
                builder.startArray(INTERVALS_FIELD.getPreferredName());
                for (TimeValue interval : intervals) {
                    builder.value(interval.getStringRep());
                }
                builder.endArray();
            }
            if (dimensions != null) {
                builder.stringListField(DIMENSIONS_FIELD.getPreferredName(), dimensions);
            }
            if (metrics != null) {
                builder.xContentList(METRICS_FIELD.getPreferredName(), metrics);
            }
            builder.endObject();
            return builder;
        }
    }

    public static class Builder {
        private Boolean enabled;
        private List<String> builtin;
        private List<TimeValue> intervals;
        private List<String> dimensions;
        private List<Metric> metrics;

        public Builder(Template template) {
            if (template != null) {
                enabled = template.enabled();
                builtin = template.builtin() == null ? null : new ArrayList<>(template.builtin());
                intervals = template.intervals() == null ? null : new ArrayList<>(template.intervals());
                dimensions = template.dimensions() == null ? null : new ArrayList<>(template.dimensions());
                metrics = template.metrics() == null ? null : new ArrayList<>(template.metrics());
            }
        }

        public Builder(DataStreamDerivedMetrics derivedMetrics) {
            enabled = derivedMetrics.enabled();
            builtin = new ArrayList<>(derivedMetrics.builtin());
            intervals = new ArrayList<>(derivedMetrics.intervals());
            dimensions = new ArrayList<>(derivedMetrics.dimensions());
            metrics = new ArrayList<>(derivedMetrics.metrics());
        }

        public Builder composeTemplate(Template template) {
            if (template == null) {
                return this;
            }
            if (template.enabled() != null) {
                enabled = template.enabled();
            }
            builtin = append(builtin, template.builtin());
            intervals = append(intervals, template.intervals());
            dimensions = append(dimensions, template.dimensions());
            metrics = mergeMetrics(metrics, template.metrics());
            return this;
        }

        public Template buildTemplate() {
            return new Template(enabled, builtin, intervals, dimensions, metrics);
        }

        public DataStreamDerivedMetrics build() {
            return DataStreamDerivedMetrics.fromTemplate(buildTemplate());
        }

        private static <T> List<T> append(@Nullable List<T> existing, @Nullable List<T> additional) {
            if (additional == null) {
                return existing;
            }
            List<T> merged = existing == null ? new ArrayList<>() : new ArrayList<>(existing);
            for (T item : additional) {
                if (merged.contains(item) == false) {
                    merged.add(item);
                }
            }
            return merged;
        }

        private static List<Metric> mergeMetrics(@Nullable List<Metric> existing, @Nullable List<Metric> additional) {
            if (additional == null) {
                return existing;
            }
            List<Metric> merged = existing == null ? new ArrayList<>() : new ArrayList<>(existing);
            Map<String, Metric> byName = new LinkedHashMap<>();
            for (Metric metric : merged) {
                byName.put(metric.name(), metric);
            }
            for (Metric metric : additional) {
                Metric current = byName.get(metric.name());
                if (current == null) {
                    byName.put(metric.name(), metric);
                    merged.add(metric);
                } else if (current.equals(metric) == false) {
                    throw new IllegalArgumentException("derived metric [" + metric.name() + "] is defined more than once");
                }
            }
            return merged;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> copyMap(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> valueMap) {
                copy.put(entry.getKey(), copyMap((Map<String, Object>) valueMap));
            } else if (value instanceof List<?> valueList) {
                copy.put(entry.getKey(), copyList(valueList));
            } else {
                copy.put(entry.getKey(), value);
            }
        }
        return Map.copyOf(copy);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> copyList(List<?> list) {
        List<Object> copy = new ArrayList<>(list.size());
        for (Object value : list) {
            if (value instanceof Map<?, ?> valueMap) {
                copy.add(copyMap((Map<String, Object>) valueMap));
            } else if (value instanceof List<?> valueList) {
                copy.add(copyList(valueList));
            } else {
                copy.add(value);
            }
        }
        return List.copyOf(copy);
    }

    @SuppressWarnings("unchecked")
    private static void validatePredicate(@Nullable Map<String, Object> predicate, String path) {
        if (predicate == null) {
            return;
        }
        if (predicate.size() != 1) {
            throw new IllegalArgumentException("derived metrics predicate [" + path + "] must contain exactly one operator");
        }
        Map.Entry<String, Object> entry = predicate.entrySet().iterator().next();
        String operator = entry.getKey();
        Object value = entry.getValue();
        switch (operator) {
            case "exists" -> validateExists(value, path);
            case "term" -> validateSingleFieldMap(value, path, false);
            case "terms" -> validateSingleFieldMap(value, path, true);
            case "range" -> validateRange(value, path);
            case "and", "or" -> {
                if (value instanceof List<?> == false) {
                    throw new IllegalArgumentException(
                        "derived metrics predicate [" + path + "." + operator + "] must be a non-empty array"
                    );
                }
                List<?> children = (List<?>) value;
                if (children.isEmpty()) {
                    throw new IllegalArgumentException(
                        "derived metrics predicate [" + path + "." + operator + "] must be a non-empty array"
                    );
                }
                for (int i = 0; i < children.size(); i++) {
                    if (children.get(i) instanceof Map<?, ?> == false) {
                        throw new IllegalArgumentException(
                            "derived metrics predicate [" + path + "." + operator + "] entries must be objects"
                        );
                    }
                    Map<String, Object> child = (Map<String, Object>) children.get(i);
                    validatePredicate(child, path + "." + operator + "[" + i + "]");
                }
            }
            case "not" -> {
                if (value instanceof Map<?, ?> == false) {
                    throw new IllegalArgumentException("derived metrics predicate [" + path + ".not] must be an object");
                }
                Map<String, Object> child = (Map<String, Object>) value;
                validatePredicate(child, path + ".not");
            }
            default -> throw new IllegalArgumentException("unsupported derived metrics predicate operator [" + operator + "]");
        }
    }

    @SuppressWarnings("unchecked")
    private static void validateExists(Object value, String path) {
        if (value instanceof Map<?, ?> == false) {
            throw new IllegalArgumentException("derived metrics predicate [" + path + ".exists] must contain a [field]");
        }
        Map<?, ?> map = (Map<?, ?>) value;
        if (map.size() != 1 || map.get("field") instanceof String == false) {
            throw new IllegalArgumentException("derived metrics predicate [" + path + ".exists] must contain a [field]");
        }
        validateFieldName((String) ((Map<String, Object>) map).get("field"), path + ".exists.field");
    }

    @SuppressWarnings("unchecked")
    private static void validateSingleFieldMap(Object value, String path, boolean requireList) {
        if (value instanceof Map<?, ?> == false) {
            throw new IllegalArgumentException("derived metrics predicate [" + path + "] must contain exactly one field");
        }
        Map<?, ?> map = (Map<?, ?>) value;
        if (map.size() != 1) {
            throw new IllegalArgumentException("derived metrics predicate [" + path + "] must contain exactly one field");
        }
        Map.Entry<String, Object> entry = ((Map<String, Object>) map).entrySet().iterator().next();
        validateFieldName(entry.getKey(), path);
        if (requireList && entry.getValue() instanceof List<?> == false) {
            throw new IllegalArgumentException("derived metrics predicate [" + path + "] must contain an array of values");
        }
    }

    @SuppressWarnings("unchecked")
    private static void validateRange(Object value, String path) {
        if (value instanceof Map<?, ?> == false) {
            throw new IllegalArgumentException("derived metrics predicate [" + path + ".range] must contain exactly one field");
        }
        Map<?, ?> map = (Map<?, ?>) value;
        if (map.size() != 1) {
            throw new IllegalArgumentException("derived metrics predicate [" + path + ".range] must contain exactly one field");
        }
        Map.Entry<String, Object> entry = ((Map<String, Object>) map).entrySet().iterator().next();
        validateFieldName(entry.getKey(), path + ".range");
        if (entry.getValue() instanceof Map<?, ?> == false) {
            throw new IllegalArgumentException("derived metrics predicate [" + path + ".range] must contain range bounds");
        }
        Map<?, ?> bounds = (Map<?, ?>) entry.getValue();
        if (bounds.isEmpty()) {
            throw new IllegalArgumentException("derived metrics predicate [" + path + ".range] must contain range bounds");
        }
        for (Map.Entry<?, ?> bound : bounds.entrySet()) {
            if (Set.of("gt", "gte", "lt", "lte").contains(bound.getKey()) == false || bound.getValue() instanceof Number == false) {
                throw new IllegalArgumentException(
                    "derived metrics predicate [" + path + ".range] supports numeric [gt], [gte], [lt], and [lte] bounds"
                );
            }
        }
    }
}
