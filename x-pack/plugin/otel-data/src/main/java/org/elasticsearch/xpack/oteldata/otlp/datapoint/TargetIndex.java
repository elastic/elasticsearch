/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.oteldata.otlp.MappingMode;

import java.util.List;
import java.util.Set;

/**
 * Represents the target index for a data point, which can be either a specific index or a data stream.
 * The index is determined based on attributes, scope name, and default values.
 */
public final class TargetIndex {

    public static final String TYPE_LOGS = "logs";
    public static final String TYPE_METRICS = "metrics";

    private static final String RECEIVER = "/receiver/";
    private static final String CONNECTOR = "/connector/";
    private static final String RECEIVER_SUFFIX = "receiver";
    private static final String CONNECTOR_SUFFIX = "connector";
    private static final Set<String> SELF_TELEMETRY_SCOPES = Set.of(
        "go.opentelemetry.io/collector/receiver/receiverhelper",
        "go.opentelemetry.io/collector/scraper/scraperhelper",
        "go.opentelemetry.io/collector/processor/processorhelper",
        "go.opentelemetry.io/collector/exporter/exporterhelper",
        "go.opentelemetry.io/collector/service"
    );
    private static final String SELF_TELEMETRY_DATASET = "collectortelemetry";
    private static final String ENCODING_FORMAT = "encoding.format";
    private static final String ELASTICSEARCH_INDEX = "elasticsearch.index";
    private static final String DATA_STREAM_TYPE = "data_stream.type";
    private static final String DATA_STREAM_DATASET = "data_stream.dataset";
    private static final String DATA_STREAM_NAMESPACE = "data_stream.namespace";
    private static final String DEFAULT_DATASET = "generic";
    private static final String OTEL_DATASET_SUFFIX = ".otel";
    private static final String DEFAULT_NAMESPACE = "default";
    private static final int MAX_DATA_STREAM_LENGTH = 100;
    private static final TargetIndex DEFAULT_METRICS_TARGET = evaluate(TYPE_METRICS, List.of(), null, List.of(), List.of());

    private String index;
    private String type;
    private String dataset;
    private String namespace;

    public static TargetIndex defaultMetrics() {
        return DEFAULT_METRICS_TARGET;
    }

    public static boolean isTargetIndexAttribute(String attributeKey) {
        return attributeKey.equals(ELASTICSEARCH_INDEX)
            || attributeKey.equals(DATA_STREAM_DATASET)
            || attributeKey.equals(DATA_STREAM_NAMESPACE);
    }

    /**
     * Determines the target index for a data point.
     *
     * @param type The data stream type (e.g., "metrics", "logs").
     * @param attributes The attributes associated with the data point.
     * @param scopeRoutingDataset The dataset derived from scope-based routing, if present.
     * @param scopeAttributes Attributes associated with the scope.
     * @param resourceAttributes Attributes associated with the resource.
     * @return A TargetIndex instance representing the target index for the data point.
     */
    public static TargetIndex evaluate(
        String type,
        List<KeyValue> attributes,
        @Nullable String scopeRoutingDataset,
        List<KeyValue> scopeAttributes,
        List<KeyValue> resourceAttributes
    ) {
        return evaluate(type, MappingMode.OTEL, attributes, scopeRoutingDataset, scopeAttributes, resourceAttributes);
    }

    /**
     * Determines the target index for a data point under a specific {@link MappingMode}.
     * <p>
     * In {@link MappingMode#BODYMAP} the {@code data_stream.type} attribute may override the default
     * {@code type} (limited to {@code logs} or {@code metrics}, mirroring the upstream collector exporter),
     * and the dataset is not suffixed with {@code .otel}.
     */
    public static TargetIndex evaluate(
        String type,
        MappingMode mode,
        List<KeyValue> attributes,
        @Nullable String scopeRoutingDataset,
        List<KeyValue> scopeAttributes,
        List<KeyValue> resourceAttributes
    ) {
        // Order:
        // 1. elasticsearch.index from attributes, scope.attributes, resource.attributes
        // 2. read data_stream.* from attributes, scope.attributes, resource.attributes
        // 3. scope-based routing
        // 4. use default hardcoded data_stream.* (<type>-generic-default)
        TargetIndex target = new TargetIndex();
        target.index = firstAttributeValue(ELASTICSEARCH_INDEX, attributes, scopeAttributes, resourceAttributes);
        if (target.index != null) {
            target.type = null;
            target.dataset = null;
            target.namespace = null;
            return target;
        }
        target.type = type;
        if (mode == MappingMode.BODYMAP) {
            String overrideType = firstAttributeValue(DATA_STREAM_TYPE, attributes, scopeAttributes, resourceAttributes);
            if (overrideType != null) {
                if (TYPE_LOGS.equals(overrideType) == false && TYPE_METRICS.equals(overrideType) == false) {
                    throw new IllegalArgumentException(
                        "data_stream.type can only be set to \"logs\" or \"metrics\", got [" + overrideType + "]"
                    );
                }
                target.type = overrideType;
            }
        }
        target.dataset = firstAttributeValue(DATA_STREAM_DATASET, attributes, scopeAttributes, resourceAttributes);
        if (target.dataset == null && scopeRoutingDataset != null) {
            target.dataset = scopeRoutingDataset;
        }
        if (target.dataset == null) {
            target.dataset = DEFAULT_DATASET;
        }
        target.dataset = sanitizeDataset(target.dataset, mode);
        target.namespace = firstAttributeValue(DATA_STREAM_NAMESPACE, attributes, scopeAttributes, resourceAttributes);
        if (target.namespace == null) {
            target.namespace = DEFAULT_NAMESPACE;
        }
        target.namespace = DataStream.sanitizeNamespace(target.namespace);
        target.index = target.type + "-" + target.dataset + "-" + target.namespace;
        return target;
    }

    public static @Nullable String extractScopeRoutingDataset(InstrumentationScope scope) {
        String scopeName = scope.getName();
        if (SELF_TELEMETRY_SCOPES.contains(scopeName)) {
            return SELF_TELEMETRY_DATASET;
        }
        for (int i = 0, size = scope.getAttributesCount(); i < size; i++) {
            KeyValue attribute = scope.getAttributes(i);
            if (ENCODING_FORMAT.equals(attribute.getKey()) && attribute.getValue().hasStringValue()) {
                String format = attribute.getValue().getStringValue();
                if (format.isEmpty() == false) {
                    return format;
                }
                break;
            }
        }
        return extractComponentName(scopeName);
    }

    private static @Nullable String extractComponentName(String scopeName) {
        String receiver = extractComponentName(scopeName, RECEIVER, RECEIVER_SUFFIX);
        if (receiver != null) {
            return receiver;
        }
        return extractComponentName(scopeName, CONNECTOR, CONNECTOR_SUFFIX);
    }

    private static @Nullable String extractComponentName(String scopeName, String marker, String requiredSuffix) {
        int indexOfMarker = scopeName.indexOf(marker);
        if (indexOfMarker < 0) {
            return null;
        }
        int beginIndex = indexOfMarker + marker.length();
        int endIndex = scopeName.indexOf('/', beginIndex);
        if (endIndex < 0) {
            endIndex = scopeName.length();
        }
        if (beginIndex >= endIndex) {
            return null;
        }
        String component = scopeName.substring(beginIndex, endIndex);
        if (component.length() <= requiredSuffix.length() || component.endsWith(requiredSuffix) == false) {
            return null;
        }
        return component;
    }

    private TargetIndex() {}

    private static @Nullable String firstAttributeValue(
        String key,
        List<KeyValue> attributes,
        List<KeyValue> scopeAttributes,
        List<KeyValue> resourceAttributes
    ) {
        String value = attributeValue(key, attributes);
        if (value != null) {
            return value;
        }
        value = attributeValue(key, scopeAttributes);
        if (value != null) {
            return value;
        }
        return attributeValue(key, resourceAttributes);
    }

    private static @Nullable String attributeValue(String key, List<KeyValue> attributes) {
        for (int i = 0, size = attributes.size(); i < size; i++) {
            KeyValue attr = attributes.get(i);
            if (attr.getKey().equals(key)) {
                return attr.getValue().getStringValue();
            }
        }
        return null;
    }

    private static String sanitizeDataset(String dataset, MappingMode mode) {
        String sanitizedDataset = DataStream.sanitizeDataset(dataset);
        String suffix = mode == MappingMode.OTEL ? OTEL_DATASET_SUFFIX : "";
        int maxBaseLength = MAX_DATA_STREAM_LENGTH - suffix.length();
        if (sanitizedDataset.length() > maxBaseLength) {
            sanitizedDataset = sanitizedDataset.substring(0, maxBaseLength);
        }
        return sanitizedDataset + suffix;
    }

    public boolean isDataStream() {
        return type != null && dataset != null && namespace != null;
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    public String dataset() {
        return dataset;
    }

    public String namespace() {
        return namespace;
    }

    @Override
    public String toString() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        TargetIndex that = (TargetIndex) o;
        return index.equals(that.index);
    }

    @Override
    public int hashCode() {
        return index.hashCode();
    }
}
