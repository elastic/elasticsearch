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

    public static final NodeFeature ASYNC_API_SUPPORTED = new NodeFeature("esql.async_api_supported");

    private static final NodeFeature MV_LOAD = new NodeFeature("esql.mv_load");

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.ofEntries(
            Map.entry(TransportEsqlStatsAction.ESQL_STATS_FEATURE, Version.V_8_11_0),
            Map.entry(MV_LOAD, Version.V_8_12_0)
        );
    }

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(ASYNC_API_SUPPORTED);
    }
}
