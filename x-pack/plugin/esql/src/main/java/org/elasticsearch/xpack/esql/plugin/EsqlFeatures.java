/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.action.admin.cluster.RestNodesCapabilitiesAction;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.Collections;
import java.util.Set;

/**
 * {@link NodeFeature}s declared by ESQL. These should be used for fast checks
 * on the node. Before the introduction of the {@link RestNodesCapabilitiesAction}
 * this was used for controlling which features are tested so many of the
 * examples below are *just* used for that. Don't make more of those - add them
 * to {@link EsqlCapabilities} instead.
 * <p>
 *     NOTE: You can't remove a feature now and probably never will be able to.
 *     Only add more of these if you need a fast CPU level check.
 * </p>
 */
public class EsqlFeatures implements FeatureSpecification {
    /**
     * Introduction of {@code MV_SORT}, {@code MV_SLICE}, and {@code MV_ZIP}.
     * Added in #106095.
     */
    private static final NodeFeature MV_SORT = new NodeFeature("esql.mv_sort", true);

    /**
     * When we disabled some broken optimizations around {@code nullable}.
     * Fixed in #105691.
     */
    private static final NodeFeature DISABLE_NULLABLE_OPTS = new NodeFeature("esql.disable_nullable_opts", true);

    /**
     * Introduction of {@code ST_X} and {@code ST_Y}. Added in #105768.
     */
    private static final NodeFeature ST_X_Y = new NodeFeature("esql.st_x_y", true);

    /**
     * Changed precision of {@code geo_point} and {@code cartesian_point} fields, by loading from source into WKB. Done in #103691.
     */
    private static final NodeFeature SPATIAL_POINTS_FROM_SOURCE = new NodeFeature("esql.spatial_points_from_source", true);

    /**
     * Support for loading {@code geo_shape} and {@code cartesian_shape} fields. Done in #104269.
     */
    private static final NodeFeature SPATIAL_SHAPES = new NodeFeature("esql.spatial_shapes", true);

    /**
     * Support for spatial aggregation {@code ST_CENTROID}. Done in #104269.
     */
    private static final NodeFeature ST_CENTROID_AGG = new NodeFeature("esql.st_centroid_agg", true);

    /**
     * Support for spatial aggregation {@code ST_INTERSECTS}. Done in #104907.
     */
    private static final NodeFeature ST_INTERSECTS = new NodeFeature("esql.st_intersects", true);

    /**
     * Support for spatial aggregation {@code ST_CONTAINS} and {@code ST_WITHIN}. Done in #106503.
     */
    private static final NodeFeature ST_CONTAINS_WITHIN = new NodeFeature("esql.st_contains_within", true);

    /**
     * Support for spatial aggregation {@code ST_DISJOINT}. Done in #107007.
     */
    private static final NodeFeature ST_DISJOINT = new NodeFeature("esql.st_disjoint", true);

    /**
     * The introduction of the {@code VALUES} agg.
     */
    private static final NodeFeature AGG_VALUES = new NodeFeature("esql.agg_values", true);

    /**
     * Does ESQL support async queries.
     */
    public static final NodeFeature ASYNC_QUERY = new NodeFeature("esql.async_query", true);

    /**
     * Does ESQL support FROM OPTIONS?
     */
    @Deprecated
    public static final NodeFeature FROM_OPTIONS = new NodeFeature("esql.from_options", true);

    /**
     * Cast string literals to a desired data type.
     */
    public static final NodeFeature STRING_LITERAL_AUTO_CASTING = new NodeFeature("esql.string_literal_auto_casting", true);

    /**
     * Base64 encoding and decoding functions.
     */
    public static final NodeFeature BASE64_DECODE_ENCODE = new NodeFeature("esql.base64_decode_encode", true);

    /**
     * Support for the :: casting operator
     */
    public static final NodeFeature CASTING_OPERATOR = new NodeFeature("esql.casting_operator", true);

    /**
     * Blocks can be labelled with {@link org.elasticsearch.compute.data.Block.MvOrdering#SORTED_ASCENDING} for optimizations.
     */
    public static final NodeFeature MV_ORDERING_SORTED_ASCENDING = new NodeFeature("esql.mv_ordering_sorted_ascending", true);

    /**
     * Support for metrics counter fields
     */
    public static final NodeFeature METRICS_COUNTER_FIELDS = new NodeFeature("esql.metrics_counter_fields", true);

    /**
     * Cast string literals to a desired data type for IN predicate and more types for BinaryComparison.
     */
    public static final NodeFeature STRING_LITERAL_AUTO_CASTING_EXTENDED = new NodeFeature(
        "esql.string_literal_auto_casting_extended",
        true
    );

    /**
     * Support for metadata fields.
     */
    public static final NodeFeature METADATA_FIELDS = new NodeFeature("esql.metadata_fields", true);

    /**
     * Support for timespan units abbreviations
     */
    public static final NodeFeature TIMESPAN_ABBREVIATIONS = new NodeFeature("esql.timespan_abbreviations", true);

    /**
     * Support metrics counter types
     */
    public static final NodeFeature COUNTER_TYPES = new NodeFeature("esql.counter_types", true);

    /**
     * Support metrics syntax
     */
    public static final NodeFeature METRICS_SYNTAX = new NodeFeature("esql.metrics_syntax");

    /**
     * Internal resolve_fields API for ES|QL
     */
    public static final NodeFeature RESOLVE_FIELDS_API = new NodeFeature("esql.resolve_fields_api", true);

    private Set<NodeFeature> snapshotBuildFeatures() {
        assert Build.current().isSnapshot() : Build.current();
        return Set.of(METRICS_SYNTAX);
    }

    @Override
    public Set<NodeFeature> getFeatures() {
        Set<NodeFeature> features = Set.of(
            ASYNC_QUERY,
            AGG_VALUES,
            BASE64_DECODE_ENCODE,
            MV_SORT,
            DISABLE_NULLABLE_OPTS,
            ST_X_Y,
            FROM_OPTIONS,
            SPATIAL_POINTS_FROM_SOURCE,
            SPATIAL_SHAPES,
            ST_CENTROID_AGG,
            ST_INTERSECTS,
            ST_CONTAINS_WITHIN,
            ST_DISJOINT,
            STRING_LITERAL_AUTO_CASTING,
            CASTING_OPERATOR,
            MV_ORDERING_SORTED_ASCENDING,
            METRICS_COUNTER_FIELDS,
            STRING_LITERAL_AUTO_CASTING_EXTENDED,
            METADATA_FIELDS,
            TIMESPAN_ABBREVIATIONS,
            COUNTER_TYPES,
            RESOLVE_FIELDS_API
        );
        if (Build.current().isSnapshot()) {
            return Collections.unmodifiableSet(Sets.union(features, snapshotBuildFeatures()));
        } else {
            return features;
        }
    }
}
