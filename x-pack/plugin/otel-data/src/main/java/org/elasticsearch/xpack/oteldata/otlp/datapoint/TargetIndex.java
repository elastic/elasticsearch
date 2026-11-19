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

import java.util.List;
import java.util.Set;

/**
 * Represents the target index for a data point, which can be either a specific index or a data stream.
 * The index is determined based on attributes, scope name, and default values.
 * <p>
 * Target indices are resolved incrementally, starting from a default (e.g. {@link #defaultForMetrics()}) and applying overrides from the
 * resource, the scope, and finally the individual record (e.g. data point, log record, or span).
 * This allows the result of the resource and scope levels to be computed once and reused for all records.
 * <p>
 * The overall order of precedence is:
 * <ol>
 *     <li>{@code elasticsearch.index} from the record, scope, or resource attributes</li>
 *     <li>{@code data_stream.*} from the record, scope, or resource attributes</li>
 *     <li>scope-based routing</li>
 *     <li>default hardcoded {@code data_stream.*} ({@code <type>-generic.otel-default})</li>
 * </ol>
 * Within the same precedence level, attributes of more specific levels take precedence over less specific ones.
 */
public final class TargetIndex {

    /**
     * The source the target index has been derived from, declared in descending order of precedence.
     */
    private enum Priority {
        ELASTICSEARCH_INDEX_ATTRIBUTES,
        DATASET_ATTRIBUTES,
        SCOPE_BASED,
        DEFAULT;

        boolean isLowerThan(Priority other) {
            return compareTo(other) > 0;
        }
    }

    public static final String TYPE_LOGS = "logs";
    public static final String TYPE_METRICS = "metrics";
    public static final String TYPE_TRACES = "traces";
    public static final String TYPE_EXEMPLARS = "exemplars";

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
    private static final String DATA_STREAM_DATASET = "data_stream.dataset";
    private static final String DATA_STREAM_NAMESPACE = "data_stream.namespace";
    private static final String DEFAULT_DATASET = "generic";
    private static final String OTEL_DATASET_SUFFIX = ".otel";
    private static final String DEFAULT_NAMESPACE = "default";
    private static final int MAX_DATA_STREAM_LENGTH = 100;
    private static final String SANITIZED_DEFAULT_DATASET = sanitizeDataset(DEFAULT_DATASET);
    private static final TargetIndex DEFAULT_METRICS = defaultFor(TYPE_METRICS);
    private static final TargetIndex DEFAULT_LOGS = defaultFor(TYPE_LOGS);
    private static final TargetIndex DEFAULT_TRACES = defaultFor(TYPE_TRACES);

    private final String index;
    private final @Nullable String type;
    private final @Nullable String dataset;
    private final @Nullable String namespace;
    private final Priority priority;

    private TargetIndex cachedExemplarsTarget = null;

    public static TargetIndex defaultForMetrics() {
        return DEFAULT_METRICS;
    }

    public static TargetIndex defaultForLogs() {
        return DEFAULT_LOGS;
    }

    public static TargetIndex defaultForTraces() {
        return DEFAULT_TRACES;
    }

    private static TargetIndex defaultFor(String type) {
        return dataStream(type, SANITIZED_DEFAULT_DATASET, DEFAULT_NAMESPACE, Priority.DEFAULT);
    }

    /**
     * Returns the exemplar data stream corresponding to this metrics data stream.
     * Explicit non-data-stream targets do not have an automatically derived exemplar target.
     */
    public @Nullable TargetIndex exemplarsTarget() {
        if (cachedExemplarsTarget != null) {
            return cachedExemplarsTarget;
        }
        if (TYPE_METRICS.equals(type) == false) {
            return null;
        }
        cachedExemplarsTarget = dataStream(TYPE_EXEMPLARS, dataset, namespace, priority);
        return cachedExemplarsTarget;
    }

    public static boolean isTargetIndexAttribute(String attributeKey) {
        return attributeKey.equals(ELASTICSEARCH_INDEX)
            || attributeKey.equals(DATA_STREAM_DATASET)
            || attributeKey.equals(DATA_STREAM_NAMESPACE);
    }

    /**
     * Applies the target index attributes ({@code elasticsearch.index}, {@code data_stream.dataset}, {@code data_stream.namespace})
     * of a more specific level (e.g. scope attributes on top of a target derived from resource attributes).
     * <p>
     * An {@code elasticsearch.index} attribute always overrides the current target.
     * {@code data_stream.*} attributes are ignored if the current target is an explicit index.
     * As namespaces have no fallback other than the default, a {@code data_stream.namespace} attribute alone doesn't change the priority.
     *
     * @return the resulting target index, or {@code this} if none of the attributes apply
     */
    public TargetIndex withAttributeOverrides(List<KeyValue> attributes) {
        String explicitIndex = null;
        String rawDataset = null;
        String rawNamespace = null;
        for (int i = 0, size = attributes.size(); i < size; i++) {
            KeyValue attribute = attributes.get(i);
            String key = attribute.getKey();
            if (key.equals(ELASTICSEARCH_INDEX)) {
                explicitIndex = attribute.getValue().getStringValue();
                break;
            } else if (priority != Priority.ELASTICSEARCH_INDEX_ATTRIBUTES) {
                if (rawDataset == null && key.equals(DATA_STREAM_DATASET)) {
                    rawDataset = attribute.getValue().getStringValue();
                } else if (rawNamespace == null && key.equals(DATA_STREAM_NAMESPACE)) {
                    rawNamespace = attribute.getValue().getStringValue();
                }
            }
        }
        if (explicitIndex != null) {
            return new TargetIndex(explicitIndex, null, null, null, Priority.ELASTICSEARCH_INDEX_ATTRIBUTES);
        }
        if (rawDataset == null && rawNamespace == null) {
            return this;
        }
        String newDataset = rawDataset != null ? sanitizeDataset(rawDataset) : dataset;
        String newNamespace = rawNamespace != null ? DataStream.sanitizeNamespace(rawNamespace) : namespace;
        Priority newPriority = rawDataset != null ? Priority.DATASET_ATTRIBUTES : priority;
        return dataStream(type, newDataset, newNamespace, newPriority);
    }

    /**
     * Applies the dataset derived from the scope name or the {@code encoding.format} scope attribute,
     * unless the current target has been derived from a source with a higher priority than scope-based routing.
     *
     * @return the resulting target index, or {@code this} if scope-based routing doesn't apply
     */
    public TargetIndex maybeApplyScopeBasedRouting(InstrumentationScope scope) {
        if (priority.isLowerThan(Priority.SCOPE_BASED) == false) {
            return this;
        }
        String scopeRoutingDataset = extractScopeRoutingDataset(scope);
        if (scopeRoutingDataset == null) {
            return this;
        }
        return dataStream(type, sanitizeDataset(scopeRoutingDataset), namespace, Priority.SCOPE_BASED);
    }

    private static @Nullable String extractScopeRoutingDataset(InstrumentationScope scope) {
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

    private TargetIndex(String index, @Nullable String type, @Nullable String dataset, @Nullable String namespace, Priority priority) {
        this.index = index;
        this.type = type;
        this.dataset = dataset;
        this.namespace = namespace;
        this.priority = priority;
    }

    /**
     * Creates a data stream target from an already sanitized dataset and namespace.
     */
    private static TargetIndex dataStream(String type, String dataset, String namespace, Priority priority) {
        return new TargetIndex(type + "-" + dataset + "-" + namespace, type, dataset, namespace, priority);
    }

    private static String sanitizeDataset(String dataset) {
        String sanitizedDataset = DataStream.sanitizeDataset(dataset);
        int maxBaseLength = MAX_DATA_STREAM_LENGTH - OTEL_DATASET_SUFFIX.length();
        if (sanitizedDataset.length() > maxBaseLength) {
            sanitizedDataset = sanitizedDataset.substring(0, maxBaseLength);
        }
        return sanitizedDataset + OTEL_DATASET_SUFFIX;
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
