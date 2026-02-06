/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.features;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.xpack.inference.InferenceFeatures;

import java.util.Objects;

/**
 * Holds ClusterService and FeatureService for checking cluster-level inference feature availability.
 */
public class InferenceFeatureService {

    private final ClusterService clusterService;
    private final FeatureService featureService;

    public InferenceFeatureService(ClusterService clusterService, FeatureService featureService) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.featureService = Objects.requireNonNull(featureService);
    }

    /**
     * Returns true if the cluster has the ENDPOINT_METADATA_FIELD feature available.
     */
    public boolean hasEndpointMetadataFeature() {
        return featureService.clusterHasFeature(clusterService.state(), InferenceFeatures.ENDPOINT_METADATA_FIELD);
    }
}
