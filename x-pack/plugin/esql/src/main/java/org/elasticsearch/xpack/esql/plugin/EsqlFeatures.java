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
     * Support for loading {@code geo_point} fields. Added in #102177.
     */
    private static final NodeFeature GEO_POINT_SUPPORT = new NodeFeature("esql.geo_point");

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

    // /**
    // * Support for loading {@code geo_point} fields.
    // */
    // private static final NodeFeature GEO_SHAPE_SUPPORT = new NodeFeature("esql.geo_shape");

    public static final NodeFeature ASYNC_QUERY = new NodeFeature("esql.async_query");

    private static final NodeFeature MV_LOAD = new NodeFeature("esql.mv_load");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(MV_SORT, DISABLE_NULLABLE_OPTS, ST_X_Y);
    }

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.ofEntries(
            Map.entry(TransportEsqlStatsAction.ESQL_STATS_FEATURE, Version.V_8_11_0),
            Map.entry(MV_WARN, Version.V_8_12_0),
            Map.entry(GEO_POINT_SUPPORT, Version.V_8_12_0),
            Map.entry(CONVERT_WARN, Version.V_8_12_0),
            Map.entry(POW_DOUBLE, Version.V_8_12_0)
            // Map.entry(GEO_SHAPE_SUPPORT, Version.V_8_13_0)
        );
    }

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(ASYNC_QUERY);
    }
}
