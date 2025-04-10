/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class SearchBusinessRulesFeatures implements FeatureSpecification {
    public static final NodeFeature PINNED_RETRIEVER_TEST_FEATURE = new NodeFeature("pinned_retriever_test");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(PINNED_RETRIEVER_TEST_FEATURE);
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(PINNED_RETRIEVER_TEST_FEATURE);
    }
}
