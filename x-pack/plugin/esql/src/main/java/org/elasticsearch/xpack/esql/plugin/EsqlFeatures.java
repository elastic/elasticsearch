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

public class EsqlFeatures implements FeatureSpecification {
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
}
