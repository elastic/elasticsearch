/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.core.inference.InferenceEndpointRegistry;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Reads registered inference endpoint ids from project custom metadata and service defaults.
 */
public class ClusterStateInferenceEndpointRegistry implements InferenceEndpointRegistry {

    private final ModelRegistry modelRegistry;

    public ClusterStateInferenceEndpointRegistry(ModelRegistry modelRegistry) {
        this.modelRegistry = Objects.requireNonNull(modelRegistry, "modelRegistry");
    }

    @Override
    public Set<String> inferenceEndpointIds(ProjectMetadata project) {
        var ids = new HashSet<>(ModelRegistryClusterStateMetadata.fromState(project).getInferenceIds());
        ids.addAll(modelRegistry.defaultEndpointIds());
        return Set.copyOf(ids);
    }

    @Override
    public boolean endpointMetadataChanged(ClusterChangedEvent event, ProjectId projectId) {
        return event.customMetadataChanged(projectId, ModelRegistryClusterStateMetadata.TYPE);
    }
}
