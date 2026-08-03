/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.core.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Checks a derived metrics configuration against the mapping of the stream it is being attached to, and rejects the combinations that
 * cannot ever work.
 *
 * <p>The rule this applies, and the reason it is narrower than it might be: <b>reject a type conflict, trust an absence.</b> A field the
 * mapping does not mention is not an error — dynamic mapping means a metric is very often configured before the first document that would
 * create the field, and mappings change under a running configuration. An absent field is a promise of data, and there is nothing to do
 * with it but wait. A field mapped as the wrong <em>type</em> is different: no future document can fix it, so the metric would emit
 * nothing for as long as it is configured, silently, and the only evidence would be a counter nobody is looking at.
 *
 * <p>Everything here is therefore skipped when the field cannot be resolved, and only fires when the mapping says something definite that
 * contradicts the configuration.
 */
public final class DerivedMetricsMappingValidator {

    private DerivedMetricsMappingValidator() {}

    /**
     * Field types a metric value can be read from. Anything outside this set yields no number however the document is written, so a
     * metric configured against one can never emit.
     */
    private static final Set<String> NUMERIC_TYPES = Set.of(
        "long",
        "integer",
        "short",
        "byte",
        "double",
        "float",
        "half_float",
        "scaled_float",
        "unsigned_long"
    );

    /** Types a {@code range} predicate can order. Everything else has no ordering the predicate could use. */
    private static final Set<String> ORDERED_TYPES = Set.of(
        "long",
        "integer",
        "short",
        "byte",
        "double",
        "float",
        "half_float",
        "scaled_float",
        "unsigned_long",
        "date",
        "date_nanos",
        "ip",
        "version"
    );

    /** Types that hold other fields rather than a value, and so cannot become a dimension. */
    private static final Set<String> CONTAINER_TYPES = Set.of("object", "nested", "passthrough");

    /**
     * @param mapping the write index's mapping, or null when the stream has no index yet — in which case nothing can be checked and
     *                nothing is rejected
     * @throws IllegalArgumentException if the configuration names a field the mapping gives a type that cannot serve it
     */
    public static void validate(String dataStream, DataStreamDerivedMetrics config, @Nullable MappingMetadata mapping) {
        if (config == null || config.enabled() == false || mapping == null) {
            return;
        }
        Map<String, Object> source = mapping.sourceAsMap();
        for (String dimension : config.dimensions()) {
            validateDimension(dataStream, "dimensions", dimension, source);
        }
        for (DataStreamDerivedMetrics.Metric metric : config.metrics()) {
            for (String dimension : metric.dimensions()) {
                validateDimension(dataStream, "metrics[" + metric.name() + "].dimensions", dimension, source);
            }
            String valueField = metric.value() == null ? null : metric.value().field();
            if (valueField != null) {
                String type = typeOf(source, valueField);
                if (type != null && NUMERIC_TYPES.contains(type) == false) {
                    throw new IllegalArgumentException(
                        reject(dataStream, "metrics[" + metric.name() + "].value.field", valueField, type)
                            + "; a metric value must be numeric, and no document can make this field one"
                    );
                }
            }
            validatePredicate(dataStream, "metrics[" + metric.name() + "].when", metric.when(), source);
        }
    }

    private static void validateDimension(String dataStream, String where, String dimension, Map<String, Object> source) {
        String type = typeOf(source, dimension);
        if (type != null && CONTAINER_TYPES.contains(type)) {
            throw new IllegalArgumentException(
                reject(dataStream, where, dimension, type) + "; a dimension must be a single value, and this field holds other fields"
            );
        }
    }

    private static void validatePredicate(String dataStream, String where, @Nullable Map<String, Object> predicate, Map<String, Object> s) {
        if (predicate == null || predicate.isEmpty()) {
            return;
        }
        // the shape is already validated when the configuration is built, so this only has to walk it
        Map.Entry<String, Object> entry = predicate.entrySet().iterator().next();
        switch (entry.getKey()) {
            case "range" -> {
                if (entry.getValue() instanceof Map<?, ?> fields) {
                    for (Map.Entry<?, ?> field : fields.entrySet()) {
                        String name = String.valueOf(field.getKey());
                        String type = typeOf(s, name);
                        if (type != null && ORDERED_TYPES.contains(type) == false) {
                            throw new IllegalArgumentException(
                                reject(dataStream, where + ".range", name, type) + "; a range predicate needs a field that can be ordered"
                            );
                        }
                    }
                }
            }
            case "and", "or" -> {
                if (entry.getValue() instanceof List<?> children) {
                    for (Object child : children) {
                        if (child instanceof Map<?, ?> map) {
                            validatePredicate(dataStream, where, asStringMap(map), s);
                        }
                    }
                }
            }
            case "not" -> {
                if (entry.getValue() instanceof Map<?, ?> map) {
                    validatePredicate(dataStream, where, asStringMap(map), s);
                }
            }
            // exists, term and terms work against any type the field may have, so there is nothing here that a mapping can contradict
            case "exists", "term", "terms" -> {
            }
            default -> {
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asStringMap(Map<?, ?> map) {
        return (Map<String, Object>) map;
    }

    /**
     * The declared type of a field path, or null when the mapping does not say.
     *
     * <p>Both spellings have to be understood, because both are legal and templates use each: a path may be nested object by object under
     * {@code properties}, or written with dots in a single key. Anything the mapping leaves to a dynamic template is unresolvable here,
     * and unresolvable means no opinion rather than a rejection.
     */
    @Nullable
    static String typeOf(Map<String, Object> mapping, String path) {
        Object root = mapping.get("properties");
        if (root instanceof Map<?, ?> == false) {
            return null;
        }
        Map<?, ?> current = (Map<?, ?>) root;
        String[] segments = path.split("\\.");
        for (int i = 0; i < segments.length; i++) {
            // the remainder of the path may be written as a single dotted key, which is as legal as nesting it object by object
            Object dotted = current.get(String.join(".", Arrays.copyOfRange(segments, i, segments.length)));
            if (dotted instanceof Map<?, ?>) {
                return declaredType((Map<?, ?>) dotted);
            }
            Object next = current.get(segments[i]);
            if (next instanceof Map<?, ?> == false) {
                return null;
            }
            Map<?, ?> node = (Map<?, ?>) next;
            if (i == segments.length - 1) {
                return declaredType(node);
            }
            Object nested = node.get("properties");
            if (nested instanceof Map<?, ?> == false) {
                return null;
            }
            current = (Map<?, ?>) nested;
        }
        return null;
    }

    /** A node with sub-fields and no declared type is an object, which is what Elasticsearch itself infers. */
    private static String declaredType(Map<?, ?> node) {
        Object type = node.get("type");
        if (type != null) {
            return String.valueOf(type).toLowerCase(Locale.ROOT);
        }
        return node.containsKey("properties") ? "object" : null;
    }

    private static String reject(String dataStream, String where, String field, String type) {
        return "derived metrics on [" + dataStream + "] cannot use [" + field + "] at [" + where + "]: it is mapped as [" + type + "]";
    }
}
