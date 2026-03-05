/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class GPUFeatures implements FeatureSpecification {

    public static final NodeFeature VECTORS_INDEXING_USE_GPU = new NodeFeature("vectors.indexing.use_gpu", true);
    public static final NodeFeature VECTORS_INDEXING_GPU_MONITORING = new NodeFeature("vectors.indexing.gpu_monitoring", true);

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(VECTORS_INDEXING_USE_GPU, VECTORS_INDEXING_GPU_MONITORING);
    }
}
