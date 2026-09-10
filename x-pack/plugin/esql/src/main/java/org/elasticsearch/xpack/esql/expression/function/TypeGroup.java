/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Named sets of {@link DataType}s that can appear in {@link Signature} declarations
 * in place of (or mixed with via {@code |}) concrete type names.
 * <p>
 *     Example: {@code params = { "NUMERIC", "STRING|ip" }}.
 * </p>
 */
public enum TypeGroup {
    /**
     * Representable numeric types used by ES|QL functions
     * ({@code integer}, {@code long}, {@code unsigned_long}, {@code double}).
     */
    NUMERIC(Arrays.stream(DataType.values()).filter(t -> t.isNumeric() && DataType.isRepresentable(t)).toList()),

    /**
     * String types: {@code keyword} and {@code text}.
     */
    STRING(List.of(DataType.KEYWORD, DataType.TEXT)),

    /**
     * Spatial geometry types: {@code geo_point}, {@code cartesian_point},
     * {@code geo_shape}, {@code cartesian_shape}.
     */
    GEO(Arrays.stream(DataType.values()).filter(DataType::isSpatial).toList()),

    /**
     * Types that support value ordering in ES|QL multivalue / top-n style functions.
     * Derived from {@link DataType#isSortable(DataType)} plus representable/supported filters.
     */
    SORTABLE(
        Arrays.stream(DataType.values())
            .filter(t -> t.supportedVersion().supportedLocally())
            .filter(DataType::isRepresentable)
            .filter(DataType::isSortable)
            .filter(t -> t != DataType.NULL)
            .filter(t -> t != DataType.DOC_DATA_TYPE && t != DataType.TSID_DATA_TYPE)
            .filter(
                t -> t != DataType.DENSE_VECTOR
                    && t != DataType.AGGREGATE_METRIC_DOUBLE
                    && t != DataType.EXPONENTIAL_HISTOGRAM
                    && t != DataType.TDIGEST
            )
            .toList()
    ),

    /**
     * All representable expression types that functions like {@code CASE} should cover.
     * Excludes internal types ({@code _doc}, {@code _tsid}).
     */
    ALL(
        Arrays.stream(DataType.values())
            .filter(t -> t.supportedVersion().supportedLocally())
            .filter(DataType::isRepresentable)
            .filter(t -> t != DataType.NULL)
            .filter(t -> t != DataType.DOC_DATA_TYPE)
            .filter(t -> t != DataType.TSID_DATA_TYPE)
            .toList()
    );

    private final List<DataType> types;

    TypeGroup(List<DataType> types) {
        this.types = List.copyOf(types);
    }

    public List<DataType> types() {
        return types;
    }

    /**
     * Resolves a group name (case-insensitive), or {@code null} if {@code name} is not a group.
     */
    public static TypeGroup parse(String name) {
        try {
            return TypeGroup.valueOf(name.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
