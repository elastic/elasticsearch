/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.util.set.Sets;

import java.util.Set;

public class EnrichPolicyMappings {

    private static final Set<String> INDEXABLE_MAPPING_FIELDS = Set.of(
        "boolean",
        "byte",
        "date",
        "date_nanos",
        "date_range",
        "dense_vector",
        "double",
        "double_range",
        "flattened",
        "float",
        "float_range",
        "geo_point",
        "geo_shape",
        "half_float",
        "integer",
        "integer_range",
        "ip",
        "ip_range",
        "keyword",
        "long",
        "long_range",
        "point",
        "scaled_float",
        "search_as_you_type",
        "shape",
        "short",
        "text",
        "token_count",
        "unsigned_long",
        "icu_collation_keyword" // Analysis ICU Plugin
    );

    private static final Set<String> REQUIRED_PARAMETER_FIELDS = Set.of("dense_vector", "scaled_float", "token_count");

    // This is just the same set of indexable fields for now
    private static final Set<String> SUPPORTED_FIELD_TYPES = Sets.union(
        INDEXABLE_MAPPING_FIELDS,
        Set.of(
            "alias",
            "binary",
            "completion",
            "nested",
            "object",
            "sparse_vector",
            "histogram", // Analytics Plugin
            "aggregate_metric_double", // Aggregate Metric Plugin
            "annotated_text", // Annotated Text Plugin
            "constant_keyword", // Constant Keyword Mapping Plugin
            "murmur3", // Murmur3 Field Plugin
            "_size", // Mapper Size Plugin
            "rank_feature", // Mapper Extras Module
            "rank_features", // Mapper Extras Module
            "match_only_text", // Mapper Extras Module
            "join", // Parent Join Module
            "percolator", // Percolator Module
            "version", // Version Field Plugin
            "wildcard" // Wildcard Plugin
        )
    );
    private static final Set<String> UNSUPPORTED_FIELD_TYPES = Set.of();

    /**
     * Fields that are supported by enrich.
     */
    public static boolean isSupportedByEnrich(String fieldType) {
        return SUPPORTED_FIELD_TYPES.contains(fieldType);
    }

    /**
     * Fields that are completely not supported by enrich.
     */
    public static boolean isUnsupportedByEnrich(String fieldType) {
        return UNSUPPORTED_FIELD_TYPES.contains(fieldType);
    }

    /**
     * Fields that have the index parameter on them and thus can have their indexing disabled.
     */
    public static boolean isIndexableByEnrich(String fieldType) {
        return INDEXABLE_MAPPING_FIELDS.contains(fieldType);
    }

    /**
     * Fields that could be supported, but they require some parameters to be set and instead of determining them for optimization we leave
     * the values off.
     */
    public static boolean isMappableByEnrich(String fieldType) {
        return REQUIRED_PARAMETER_FIELDS.contains(fieldType) == false;
    }
}
