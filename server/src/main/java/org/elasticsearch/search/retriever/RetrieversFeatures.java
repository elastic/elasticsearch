/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * Each retriever is given its own {@link NodeFeature} so new
 * retrievers can be added individually with additional functionality.
 */
public class RetrieversFeatures implements FeatureSpecification {
    public static final NodeFeature NEGATIVE_RANK_WINDOW_SIZE_FIX = new NodeFeature("retriever.negative_rank_window_size_fix");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(
            RetrieverBuilder.RETRIEVERS_SUPPORTED,
            StandardRetrieverBuilder.STANDARD_RETRIEVER_SUPPORTED,
            KnnRetrieverBuilder.KNN_RETRIEVER_SUPPORTED
        );
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(NEGATIVE_RANK_WINDOW_SIZE_FIX);
    }
}
