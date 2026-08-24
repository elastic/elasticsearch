/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ClusterStateInferenceEndpointRegistryTests extends ESTestCase {

    public void testInferenceEndpointIdsReturnsRegistryKeys() {
        ModelRegistryClusterStateMetadata metadata = ModelRegistryClusterStateMetadataTests.randomInstance();
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.fromId("project-a"))
            .putCustom(ModelRegistryClusterStateMetadata.TYPE, metadata)
            .build();

        assertThat(new ClusterStateInferenceEndpointRegistry().inferenceEndpointIds(project), equalTo(metadata.getInferenceIds()));
    }
}
