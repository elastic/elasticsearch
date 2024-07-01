/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;

import java.util.Set;

public final class SearchFeatures implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(KnnVectorQueryBuilder.K_PARAM_SUPPORTED);
    }
}
