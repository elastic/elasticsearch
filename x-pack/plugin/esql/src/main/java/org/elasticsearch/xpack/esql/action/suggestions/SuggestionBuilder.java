/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions;

import org.elasticsearch.xpack.esql.action.EsqlSuggestionsResponse.FieldSuggestion;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Turns an analyzed plan node's output schema into the coordinator-only {@code fields} map (types
 * only, no statistics). Also classifies which statistic a field is eligible for, so that a future
 * data-node visit knows whether to sample {@code values}, a {@code range}, or nothing.
 */
public final class SuggestionBuilder {

    /** Which statistic a field's type is eligible to carry once data nodes are visited. */
    public enum StatisticKind {
        /** {@code keyword}, {@code boolean}, {@code ip} — discrete sampled values. */
        VALUES,
        /** date/numeric types — a min/max range. */
        RANGE,
        /** {@code text} and everything else — no statistics ever. */
        NONE
    }

    private SuggestionBuilder() {}

    /**
     * Build the {@code fields} map from a schema-source plan node. Keys are field names in schema
     * order; each value carries just the resolved type. Duplicate names keep the first occurrence.
     */
    public static Map<String, FieldSuggestion> fieldsFromSchema(LogicalPlan schemaSource) {
        Map<String, FieldSuggestion> fields = new LinkedHashMap<>();
        for (Attribute attribute : schemaSource.output()) {
            // On a parsed-but-unanalyzed plan the coordinator cannot resolve a field's type; skip such
            // attributes rather than fail. A fully analyzed plan resolves every attribute.
            if (attribute.resolved() == false) {
                continue;
            }
            fields.putIfAbsent(attribute.name(), FieldSuggestion.ofType(wireType(attribute.dataType())));
        }
        return fields;
    }

    /** The wire type name for a data type, matching the ESQL response convention. */
    public static String wireType(DataType type) {
        return type.outputType();
    }

    /**
     * Classify the statistic a type is eligible for. {@code keyword}/{@code boolean}/{@code ip} get
     * discrete values; date and numeric types get a range; {@code text} (and anything without a
     * meaningful sample) gets nothing.
     */
    public static StatisticKind statisticFor(DataType type) {
        if (type == DataType.KEYWORD || type == DataType.BOOLEAN || type == DataType.IP) {
            return StatisticKind.VALUES;
        }
        if (type == DataType.TEXT) {
            return StatisticKind.NONE;
        }
        if (isDate(type) || type.isNumeric()) {
            return StatisticKind.RANGE;
        }
        return StatisticKind.NONE;
    }

    private static boolean isDate(DataType type) {
        return type == DataType.DATETIME || type == DataType.DATE_NANOS;
    }
}
