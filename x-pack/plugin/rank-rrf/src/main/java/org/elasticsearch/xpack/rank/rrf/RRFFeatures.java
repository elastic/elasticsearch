/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

import static org.elasticsearch.xpack.rank.rrf.RRFRetrieverBuilder.RRF_RETRIEVER_COMPOSITION_SUPPORTED;

/**
 * A set of features specifically for the rrf plugin.
 */
public class RRFFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(RRFRetrieverBuilder.RRF_RETRIEVER_SUPPORTED, RRF_RETRIEVER_COMPOSITION_SUPPORTED);
    }
}
