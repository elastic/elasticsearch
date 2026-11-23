/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Shared naming conventions for PromQL labels represented as ES|QL fields.
 */
public final class PromqlLabels {
    private PromqlLabels() {}

    public static final String PROMETHEUS_LABELS_PREFIX = "labels.";

    /**
     * Whether {@code name} is a valid PromQL label name, used at analysis time to validate the destination (and, for
     * {@code label_join}, source) label names of {@code label_replace}/{@code label_join} and the label of
     * {@code count_values}. This mirrors the UTF-8 label-name validation Prometheus applies to these functions - a label
     * name is valid when it is non-empty - rather than the legacy {@code [a-zA-Z_][a-zA-Z0-9_]*} grammar. Derived labels
     * are materialized as ordinary ES|QL columns, which admit dotted names such as {@code service.name}, so the legacy
     * grammar would reject names that Prometheus and the surrounding query both accept.
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

    /** The label names of {@code attributes}, in order and without repeats. */
    public static List<String> labelNames(Collection<? extends Attribute> attributes) {
        return attributes.stream().map(PromqlLabels::labelName).distinct().toList();
    }

    /** The label names {@code names} as the sorted keyword literals the label container functions take. */
    public static List<Expression> labelLiterals(Source source, Collection<String> names) {
        return new TreeSet<>(names).stream().<Expression>map(name -> Literal.keyword(source, name)).toList();
    }

    /** The label container {@code labels} without {@code names}; null, a vector without labels, stays null. */
    public static @Nullable Expression without(@Nullable Expression labels, Collection<String> names) {
        return labels == null ? null : new TimeSeriesMetadataFilter(labels.source(), labels, labelLiterals(labels.source(), names), true);
    }

    /** The label container {@code labels} with only {@code names}; null, a vector without labels, stays null. */
    public static @Nullable Expression keeping(@Nullable Expression labels, Collection<String> names) {
        return labels == null ? null : new TimeSeriesMetadataFilter(labels.source(), labels, labelLiterals(labels.source(), names), false);
    }

    /** The value of label {@code name} of the label container {@code labels}. */
    public static Expression read(Expression labels, String name) {
        return new TimeSeriesMetadataPath(labels.source(), labels, Literal.keyword(labels.source(), name));
    }

    /**
     * The keys of the source field {@code field}, the path a label written into a label container takes so that it reads as the
     * source holds it. A passthrough object of the {@code resolutions} holds its fields by their names below it, dots and all,
     * as the synthetic source writes them, and every other object nests at each dot: remote write keeps {@code labels.pod} at
     * {@code [labels, pod]}.
     */
    public static List<String> path(String field, Collection<IndexResolution> resolutions) {
        List<String> keys = List.of(field.split("\\."));
        for (IndexResolution resolution : resolutions) {
            if (resolution.isValid() == false) {
                continue;
            }
            Map<String, EsField> mapping = resolution.get().mapping();
            for (int i = 0; i < keys.size() - 1 && mapping.get(keys.get(i)) instanceof EsField object; i++) {
                if (object.isPassthrough()) {
                    var path = new ArrayList<>(keys.subList(0, i + 1));
                    path.add(String.join(".", keys.subList(i + 1, keys.size())));
                    return path;
                }
                mapping = object.getProperties();
            }
        }
        return keys;
    }

    /** The label container {@code labels} without the metric name, as most functions and operators leave it. */
    public static @Nullable Expression withoutMetricName(@Nullable Expression labels) {
        return without(labels, List.of(LabelMatcher.NAME));
    }
}
