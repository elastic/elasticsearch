/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class RankVectorsFeatures implements FeatureSpecification {
    public static final NodeFeature RANK_VECTORS_FEATURE = new NodeFeature("rank_vectors");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(RANK_VECTORS_FEATURE);
    }

}
