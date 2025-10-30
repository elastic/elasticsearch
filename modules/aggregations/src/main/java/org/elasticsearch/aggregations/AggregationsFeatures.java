/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public final class AggregationsFeatures implements FeatureSpecification {

    public static final NodeFeature NESTED_AGG_TOP_HITS_WITH_INNER_HITS = new NodeFeature("nested_agg_top_hits_with_inner_hits");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(NESTED_AGG_TOP_HITS_WITH_INNER_HITS);
    }
}
