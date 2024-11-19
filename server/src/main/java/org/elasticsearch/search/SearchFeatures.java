/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;

import java.util.Set;

public final class SearchFeatures implements FeatureSpecification {

    public static final NodeFeature LUCENE_10_0_0_UPGRADE = new NodeFeature("lucene_10_upgrade");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(KnnVectorQueryBuilder.K_PARAM_SUPPORTED, LUCENE_10_0_0_UPGRADE);
    }
}
