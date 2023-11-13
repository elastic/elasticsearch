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
    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of(TransportEsqlStatsAction.ESQL_STATS_FEATURE, Version.V_8_11_0);
    }
}
