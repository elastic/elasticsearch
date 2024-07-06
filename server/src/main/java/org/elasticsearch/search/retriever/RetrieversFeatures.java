/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(
            RetrieverBuilder.RETRIEVERS_SUPPORTED,
            StandardRetrieverBuilder.STANDARD_RETRIEVER_SUPPORTED,
            KnnRetrieverBuilder.KNN_RETRIEVER_SUPPORTED
        );
    }
}
