/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Map;
import java.util.Set;

public class HealthFeatures implements FeatureSpecification {

    public static final NodeFeature SUPPORTS_HEALTH = new NodeFeature("health.supports_health");
    public static final NodeFeature SUPPORTS_SHARDS_CAPACITY_INDICATOR = new NodeFeature("health.shards_capacity_indicator");
    public static final NodeFeature SUPPORTS_EXTENDED_REPOSITORY_INDICATOR = new NodeFeature("health.extended_repository_indicator");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(SUPPORTS_EXTENDED_REPOSITORY_INDICATOR);
    }

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of(SUPPORTS_HEALTH, Version.V_8_5_0, SUPPORTS_SHARDS_CAPACITY_INDICATOR, Version.V_8_8_0);
    }
}
