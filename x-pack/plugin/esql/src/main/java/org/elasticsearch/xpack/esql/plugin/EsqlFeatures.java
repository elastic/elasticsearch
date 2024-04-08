/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Map;
import java.util.Set;

public class EsqlFeatures implements FeatureSpecification {
    /**
     * Introduction of {@code MV_SORT}, {@code MV_SLICE}, and {@code MV_ZIP}.
     * Added in #106095.
     */
    private static final NodeFeature MV_SORT = new NodeFeature("esql.mv_sort");

    /**
     * When we disabled some broken optimizations around {@code nullable}.
     * Fixed in #105691.
     */
    private static final NodeFeature DISABLE_NULLABLE_OPTS = new NodeFeature("esql.disable_nullable_opts");

    /**
     * Introduction of {@code ST_X} and {@code ST_Y}. Added in #105768.
     */
    private static final NodeFeature ST_X_Y = new NodeFeature("esql.st_x_y");

    /**
     * When we added the warnings for multivalued fields emitting {@code null}
     * when they touched multivalued fields. Added in #102417.
     */
    private static final NodeFeature MV_WARN = new NodeFeature("esql.mv_warn");

    /**
     * Support for loading {@code geo_point} and {@code cartesian_point} fields. Added in #102177.
     */
    private static final NodeFeature SPATIAL_POINTS = new NodeFeature("esql.spatial_points");

    /**
     * Changed precision of {@code geo_point} and {@code cartesian_point} fields, by loading from source into WKB. Done in #103691.
     */
    private static final NodeFeature SPATIAL_POINTS_FROM_SOURCE = new NodeFeature("esql.spatial_points_from_source");

    /**
     * When we added the warnings when conversion functions fail. Like {@code TO_INT('foo')}.
     * Added in ESQL-1183.
     */
    private static final NodeFeature CONVERT_WARN = new NodeFeature("esql.convert_warn");

    /**
     * When we flipped the return type of {@code POW} to always return a double. Changed
     * in #102183.
     */
    private static final NodeFeature POW_DOUBLE = new NodeFeature("esql.pow_double");

    /**
     * Support for loading {@code geo_shape} and {@code cartesian_shape} fields. Done in #104269.
     */
    private static final NodeFeature SPATIAL_SHAPES = new NodeFeature("esql.spatial_shapes");

    /**
     * Support for spatial aggregation {@code ST_CENTROID}. Done in #104269.
     */
    private static final NodeFeature ST_CENTROID = new NodeFeature("esql.st_centroid");

    /**
     * Support for spatial aggregation {@code ST_INTERSECTS}. Done in #104907.
     */
    private static final NodeFeature ST_INTERSECTS = new NodeFeature("esql.st_intersects");

    /**
     * Support for spatial aggregation {@code ST_CONTAINS} and {@code ST_WITHIN}. Done in #106503.
     */
    private static final NodeFeature ST_CONTAINS_WITHIN = new NodeFeature("esql.st_contains_within");

    /**
     * The introduction of the {@code VALUES} agg.
     */
    private static final NodeFeature AGG_VALUES = new NodeFeature("esql.agg_values");

    /**
     * Does ESQL support async queries.
     */
    public static final NodeFeature ASYNC_QUERY = new NodeFeature("esql.async_query");

    /**
     * Does ESQL support FROM OPTIONS?
     */
    public static final NodeFeature FROM_OPTIONS = new NodeFeature("esql.from_options");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(
            ASYNC_QUERY,
            AGG_VALUES,
            MV_SORT,
            DISABLE_NULLABLE_OPTS,
            ST_X_Y,
            FROM_OPTIONS,
            SPATIAL_POINTS_FROM_SOURCE,
            SPATIAL_SHAPES,
            ST_CENTROID,
            ST_INTERSECTS,
            ST_CONTAINS_WITHIN
        );
    }

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.ofEntries(
            Map.entry(TransportEsqlStatsAction.ESQL_STATS_FEATURE, Version.V_8_11_0),
            Map.entry(MV_WARN, Version.V_8_12_0),
            Map.entry(SPATIAL_POINTS, Version.V_8_12_0),
            Map.entry(CONVERT_WARN, Version.V_8_12_0),
            Map.entry(POW_DOUBLE, Version.V_8_12_0)
        );
    }
}
