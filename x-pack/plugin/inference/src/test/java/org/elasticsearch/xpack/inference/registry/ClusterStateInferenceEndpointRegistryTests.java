/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.inference.EndpointClusterStateTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterStateInferenceEndpointRegistryTests extends ESTestCase {

    public void testInferenceEndpointIdsReturnsRegistryKeys() {
        ModelRegistryClusterStateMetadata metadata = ModelRegistryClusterStateMetadataTests.randomInstance();
        ModelRegistry modelRegistry = mock(ModelRegistry.class);
        when(modelRegistry.defaultEndpointIds()).thenReturn(Set.of());
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.fromId("project-a"))
            .putCustom(ModelRegistryClusterStateMetadata.TYPE, metadata)
            .build();

        assertThat(
            new ClusterStateInferenceEndpointRegistry(modelRegistry).inferenceEndpointIds(project),
            equalTo(metadata.getInferenceIds())
        );
    }

    public void testInferenceEndpointIdsIncludesDefaultEndpointIds() {
        ModelRegistryClusterStateMetadata metadata = ModelRegistryClusterStateMetadata.EMPTY_UPGRADED;
        ModelRegistry modelRegistry = mock(ModelRegistry.class);
        when(modelRegistry.defaultEndpointIds()).thenReturn(Set.of(ElasticsearchInternalService.DEFAULT_ELSER_ID));
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.fromId("project-a"))
            .putCustom(ModelRegistryClusterStateMetadata.TYPE, metadata)
            .build();

        assertThat(
            new ClusterStateInferenceEndpointRegistry(modelRegistry).inferenceEndpointIds(project),
            equalTo(Set.of(ElasticsearchInternalService.DEFAULT_ELSER_ID))
        );
    }

    public void testEndpointMetadataChangedReturnsTrueWhenModelRegistryMetadataChanges() {
        ModelRegistry modelRegistry = mock(ModelRegistry.class);
        when(modelRegistry.defaultEndpointIds()).thenReturn(Set.of());
        ClusterStateInferenceEndpointRegistry registry = new ClusterStateInferenceEndpointRegistry(modelRegistry);

        ProjectId projectId = ProjectId.fromId("project-a");
        ModelRegistryClusterStateMetadata previousMetadata = ModelRegistryClusterStateMetadata.EMPTY_UPGRADED;
        ModelRegistryClusterStateMetadata currentMetadata = previousMetadata.withAddedModel(
            "endpoint-a",
            EndpointClusterStateTests.randomInstance()
        );
        ProjectMetadata previousProject = ProjectMetadata.builder(projectId)
            .putCustom(ModelRegistryClusterStateMetadata.TYPE, previousMetadata)
            .build();
        ProjectMetadata currentProject = ProjectMetadata.builder(projectId)
            .putCustom(ModelRegistryClusterStateMetadata.TYPE, currentMetadata)
            .build();
        ClusterState previous = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(previousProject).build())
            .build();
        ClusterState current = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(currentProject).build())
            .build();

        assertThat(registry.endpointMetadataChanged(new ClusterChangedEvent("test", current, previous), projectId), equalTo(true));
    }

    public void testEndpointMetadataChangedReturnsFalseWhenUnrelatedMetadataChanges() {
        ModelRegistry modelRegistry = mock(ModelRegistry.class);
        when(modelRegistry.defaultEndpointIds()).thenReturn(Set.of());
        ClusterStateInferenceEndpointRegistry registry = new ClusterStateInferenceEndpointRegistry(modelRegistry);

        ProjectId projectId = ProjectId.fromId("project-a");
        ModelRegistryClusterStateMetadata metadata = ModelRegistryClusterStateMetadata.EMPTY_UPGRADED;
        ProjectMetadata project = ProjectMetadata.builder(projectId).putCustom(ModelRegistryClusterStateMetadata.TYPE, metadata).build();
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(project).build()).build();

        assertThat(registry.endpointMetadataChanged(new ClusterChangedEvent("test", state, state), projectId), equalTo(false));
    }
}
