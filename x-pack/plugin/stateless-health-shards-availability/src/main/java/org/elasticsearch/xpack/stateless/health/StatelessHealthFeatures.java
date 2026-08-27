/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.health;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class StatelessHealthFeatures implements FeatureSpecification {

    public static final NodeFeature STATELESS_SHARDS_AVAILABILITY = new NodeFeature("health.stateless_shards_availability");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(STATELESS_SHARDS_AVAILABILITY);
    }
}
