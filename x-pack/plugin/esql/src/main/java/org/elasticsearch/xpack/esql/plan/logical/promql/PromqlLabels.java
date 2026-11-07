/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Shared naming conventions for PromQL labels represented as ES|QL fields.
 */
public final class PromqlLabels {
    private PromqlLabels() {}

    public static final String PROMETHEUS_LABELS_PREFIX = "labels.";

    /**
     * Whether {@code name} is a valid PromQL label name, used at analysis time to validate the destination (and, for
     * {@code label_join}, source) label names of {@code label_replace}/{@code label_join}. This mirrors the UTF-8
     * label-name validation Prometheus applies to these functions - a label name is valid when it is non-empty - rather
     * than the legacy {@code [a-zA-Z_][a-zA-Z0-9_]*} grammar. Derived labels are materialized as ordinary ES|QL columns,
     * which admit dotted names such as {@code service.name}, so the legacy grammar would reject names that Prometheus and
     * the surrounding query both accept.
     */
    public static boolean isValidLabelName(String name) {
        return name != null && name.isEmpty() == false;
    }

    /**
     * The PromQL label name an attribute carries: a {@link FieldAttribute}'s backing field name, otherwise the attribute
     * name, with the {@code labels.} passthrough prefix stripped so {@code labels.pod} and a bare {@code pod} compare equal.
     */
    public static String labelName(Attribute attribute) {
        return labelName(attribute instanceof FieldAttribute field ? field.fieldName().string() : attribute.name());
    }

    /** Canonical label name for a mapped source field. */
    public static String labelName(String name) {
        return name.startsWith(PROMETHEUS_LABELS_PREFIX) ? name.substring(PROMETHEUS_LABELS_PREFIX.length()) : name;
    }

    /** labels container the plan does not promote, or null when every label is promoted. */
    public static @Nullable UnresolvedPromqlLabelRef metadataLabel(List<Attribute> labels) {
        for (var a : labels) {
            if (isMetadataLabel(a) && a instanceof UnresolvedPromqlLabelRef ua) {
                return ua;
            }
        }
        return null;
    }

    /** Whether the label is _timeseries metadata */
    public static boolean isMetadataLabel(Attribute label) {
        return label instanceof UnresolvedPromqlLabelRef reference && reference.isMetadata();
    }

    /** Column name for metadata after applying unset (e.g. _timeseries$pod after without(pod)). */
    public static String metadataLabelName(Set<String> unset) {
        String suffix = new TreeSet<>(unset).stream()
            .map(label -> label.replace("\\", "\\\\").replace("$", "\\$"))
            .collect(Collectors.joining("$"));
        return MetadataAttribute.TIMESERIES + (unset.isEmpty() ? "" : "$" + suffix);
    }

    /**
     * Finds a canonical label, preferring its backing field ({@code labels.x}) to a same-named bare reference. Matches by
     * name only: a bare dimension of any type ({@code long}, {@code ip}, {@code boolean}) is a label like a keyword one, and
     * the folded step column is found by its name too. A label colliding with the sample {@code value} / {@code step}
     * columns is rejected by the verifier before translation, so no type check is needed here.
     */
    public static Attribute getLabel(List<Attribute> attributes, String label) {
        Attribute bareMatch = null;
        for (Attribute attribute : attributes) {
            if (labelName(attribute instanceof FieldAttribute field ? field.fieldName().string() : attribute.name()).equals(label)) {
                if (attribute.name().equals(label) == false) {
                    return attribute;
                }
                if (bareMatch == null) {
                    bareMatch = attribute;
                }
            }
        }
        return bareMatch;
    }

    /**
     * The dimension of {@code source} carrying {@code label}, by canonical name, or null when the source has none.
     * Returns any dimension type (including non-string): callers that require PromQL string labels must check
     * {@link DataType#isString} themselves.
     */
    public static @Nullable Attribute getLabel(LogicalPlan source, String label) {
        for (Attribute attribute : source.output()) {
            if (attribute instanceof FieldAttribute field
                && field.isDimension()
                && (attribute instanceof TimeSeriesMetadataAttribute) == false
                && labelName(attribute).equals(label)) {
                return attribute;
            }
        }
        return null;
    }
}
