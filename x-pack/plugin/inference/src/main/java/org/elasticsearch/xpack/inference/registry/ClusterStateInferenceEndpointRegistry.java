/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.core.inference.InferenceEndpointRegistry;

import java.util.Set;

/**
 * Reads registered inference endpoint ids from project custom metadata.
 */
public class ClusterStateInferenceEndpointRegistry implements InferenceEndpointRegistry {

    @Override
    public Set<String> inferenceEndpointIds(ProjectMetadata project) {
        return ModelRegistryClusterStateMetadata.fromState(project).getInferenceIds();
    }
}
