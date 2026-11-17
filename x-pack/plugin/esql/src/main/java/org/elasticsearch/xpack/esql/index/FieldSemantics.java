/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * What the mappings say about how a field should be used, merged across the indices of a pattern.
 * <p>
 * Built from field caps on the coordinator and never serialized. It backs {@code DESCRIBE} and the
 * {@code _meta.field} column metadata, so agents can tell a counter from a gauge, a dimension from a
 * free-text field, and read units, without a second API.
 *
 * @param field        the field name in the mapping
 * @param role         how the field is meant to be used, see {@link Role}
 * @param metricType   {@code counter}, {@code gauge} or {@code histogram} when the mapping declares {@code time_series_metric}
 * @param units        every {@code meta.unit} of the field; more than one when the indices disagree
 * @param descriptions every {@code meta.description} of the field; more than one when the indices disagree
 */
public record FieldSemantics(
    String field,
    Role role,
    @Nullable String metricType,
    SortedSet<String> units,
    SortedSet<String> descriptions
) {

    /**
     * How a field is meant to be used. Declared roles ({@link #DIMENSION}, {@link #METRIC}) come from
     * time-series mappings; the others are inferred from the field type.
     */
    public enum Role {
        TIME,
        DIMENSION,
        METRIC,
        ATTRIBUTE,
        NUMERIC,
        TEXT,
        SEMANTIC,
        VECTOR,
        METADATA,
        OTHER,
        /** The indices disagree on the role. */
        MIXED;

        public String label() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public FieldSemantics {
        Objects.requireNonNull(field);
        Objects.requireNonNull(role);
        units = Collections.unmodifiableSortedSet(new TreeSet<>(units));
        descriptions = Collections.unmodifiableSortedSet(new TreeSet<>(descriptions));
    }

    /**
     * Merges the field caps of one field across every index it is mapped in.
     */
    public static FieldSemantics from(String field, List<IndexFieldCapabilities> fcs) {
        Role role = null;
        String metricType = null;
        TreeSet<String> units = new TreeSet<>();
        TreeSet<String> descriptions = new TreeSet<>();
        for (IndexFieldCapabilities fc : fcs) {
            Role next = role(field, fc);
            role = role == null || role == next ? next : Role.MIXED;
            if (fc.metricType() != null) {
                String type = fc.metricType().toString();
                metricType = metricType == null || metricType.equals(type) ? type : "mixed";
            }
            addMeta(fc.meta(), "unit", units);
            addMeta(fc.meta(), "description", descriptions);
        }
        return new FieldSemantics(field, role == null ? Role.OTHER : role, metricType, units, descriptions);
    }

    /**
     * Merges the semantics of one field read by two branches of a query, as {@link #from} merges indices.
     * Returns null when the branches read different fields.
     */
    @Nullable
    public static FieldSemantics merge(FieldSemantics a, FieldSemantics b) {
        if (a.field.equals(b.field) == false) {
            return null;
        }
        Role role = a.role == b.role ? a.role : Role.MIXED;
        String metricType = a.metricType == null ? b.metricType
            : b.metricType == null || a.metricType.equals(b.metricType) ? a.metricType
            : "mixed";
        return new FieldSemantics(a.field, role, metricType, union(a.units, b.units), union(a.descriptions, b.descriptions));
    }

    private static SortedSet<String> union(SortedSet<String> a, SortedSet<String> b) {
        TreeSet<String> values = new TreeSet<>(a);
        values.addAll(b);
        return values;
    }

    private static Role role(String field, IndexFieldCapabilities fc) {
        if (fc.isMetadatafield()) {
            return Role.METADATA;
        }
        if (fc.isDimension()) {
            return Role.DIMENSION;
        }
        if (fc.metricType() != null) {
            return Role.METRIC;
        }
        if (fc.isInference()) {
            return Role.SEMANTIC;
        }
        return switch (fc.type()) {
            case "date", "date_nanos" -> MetadataAttribute.TIMESTAMP_FIELD.equals(field) ? Role.TIME : Role.ATTRIBUTE;
            case "keyword", "constant_keyword", "wildcard", "ip", "boolean", "version", "geo_point", "geo_shape" -> Role.ATTRIBUTE;
            case "long", "integer", "short", "byte", "double", "float", "half_float", "scaled_float", "unsigned_long",
                "aggregate_metric_double", "histogram", "exponential_histogram" -> Role.NUMERIC;
            case "text", "match_only_text", "pattern_text" -> Role.TEXT;
            case "semantic_text" -> Role.SEMANTIC;
            case "dense_vector", "sparse_vector", "rank_vectors" -> Role.VECTOR;
            default -> Role.OTHER;
        };
    }

    private static void addMeta(Map<String, String> meta, String key, TreeSet<String> into) {
        if (meta == null) {
            return;
        }
        String value = meta.get(key);
        if (value != null && value.isBlank() == false) {
            into.add(value);
        }
    }

    /**
     * A single value as is, or every value as a list, as ES|QL reports a multi-valued column.
     */
    public static Object singleOrList(Collection<String> values) {
        return values.size() == 1 ? values.iterator().next() : List.copyOf(values);
    }

    /**
     * The {@code _meta.field} entry of a result column.
     */
    public Map<String, Object> toColumnMetadata() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", field);
        map.put("role", role.label());
        if (metricType != null) {
            map.put("metric_type", metricType);
        }
        if (descriptions.isEmpty() == false) {
            map.put("description", singleOrList(descriptions));
        }
        return map;
    }
}
