/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Label naming and identification shared by PromQL translators.
 */
public final class PromqlLabels {
    private PromqlLabels() {}

    public static final String PROMETHEUS_LABELS_PREFIX = "labels.";

    /** Whether the output carries labels in time-series metadata instead of exposing every label as a named column. */
    public static boolean hasTsMetadata(Collection<? extends Attribute> attributes) {
        return attributes.stream().anyMatch(PromqlLabels::isTsMetadata);
    }

    /** A label may be named {@code _timeseries}; only the metadata carrier represents an unknown label set. */
    public static boolean isTsMetadata(Attribute attribute) {
        return attribute instanceof TimeSeriesMetadataAttribute
            || (attribute.resolved() && attribute.dataType() == DataType.TIMESERIES_DATA_TYPE);
    }

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
     * A mapped field's PromQL label name strips the {@code labels.} passthrough prefix. Derived labels retain their
     * declared name, so a derived {@code labels.pod} remains distinct from the mapped label {@code pod}.
     */
    public static String labelName(Attribute attribute) {
        return attribute instanceof FieldAttribute field ? labelName(field.fieldName().string()) : attribute.name();
    }

    /** Canonical label name for a mapped source field. */
    public static String labelName(String name) {
        return name.startsWith(PROMETHEUS_LABELS_PREFIX) ? name.substring(PROMETHEUS_LABELS_PREFIX.length()) : name;
    }

    /** Canonical label names in declaration order, without duplicates. */
    public static List<String> labelNames(Collection<? extends Attribute> attributes) {
        var strings = new LinkedHashSet<String>();
        for (var attribute : attributes) {
            strings.add(labelName(attribute));
        }
        return new ArrayList<>(strings);
    }

}
