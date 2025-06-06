/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

import static org.elasticsearch.search.retriever.CompoundRetrieverBuilder.INNER_RETRIEVERS_FILTER_SUPPORT;
import static org.elasticsearch.xpack.rank.linear.MinMaxScoreNormalizer.LINEAR_RETRIEVER_MINMAX_SINGLE_DOC_FIX;

public class RankRRFFeatures implements FeatureSpecification {

    public static final NodeFeature LINEAR_RETRIEVER_SUPPORTED = new NodeFeature("linear_retriever_supported");
    public static final NodeFeature SIMPLIFIED_RETRIEVER_FORMAT = new NodeFeature("simplified_retriever_format");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(LINEAR_RETRIEVER_SUPPORTED);
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(INNER_RETRIEVERS_FILTER_SUPPORT, LINEAR_RETRIEVER_MINMAX_SINGLE_DOC_FIX, SIMPLIFIED_RETRIEVER_FORMAT);
    }
}
