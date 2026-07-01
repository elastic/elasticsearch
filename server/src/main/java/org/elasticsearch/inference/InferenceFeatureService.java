/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;

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
     * Returns true if the cluster has the provided feature available.
     */
    public boolean hasFeature(NodeFeature nodeFeature) {
        return featureService.clusterHasFeature(clusterService.state(), nodeFeature);
    }

    /**
     * Returns true if the provided cluster state indicates the feature is available cluster-wide.
     */
    public boolean hasFeature(ClusterState state, NodeFeature nodeFeature) {
        return featureService.clusterHasFeature(state, nodeFeature);
    }
}
