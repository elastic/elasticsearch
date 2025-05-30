/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

import static org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.SPARSE_VECTOR_INDEX_OPTIONS_FEATURE;

/**
 * Provides the XPack features that this version of the code supports
 */
public class XPackFeatures implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(SPARSE_VECTOR_INDEX_OPTIONS_FEATURE);
    }
}
