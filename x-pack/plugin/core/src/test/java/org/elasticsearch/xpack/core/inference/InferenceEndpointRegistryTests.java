/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class InferenceEndpointRegistryTests extends ESTestCase {

    @After
    public void resetRegistry() {
        InferenceEndpointRegistry.setInstance(InferenceEndpointRegistry.Noop.INSTANCE);
    }

    public void testNoopRegistryReturnsEmptySet() {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.fromId("project-a")).build();
        assertThat(InferenceEndpointRegistry.getInstance().inferenceEndpointIds(project), equalTo(Set.of()));
    }

    public void testSetInstanceReplacesRegistry() {
        InferenceEndpointRegistry custom = project -> Set.of("endpoint-a");
        InferenceEndpointRegistry.setInstance(custom);
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.fromId("project-a")).build();
        assertThat(InferenceEndpointRegistry.getInstance().inferenceEndpointIds(project), equalTo(Set.of("endpoint-a")));
    }

    public void testSetInstanceRejectsNull() {
        expectThrows(NullPointerException.class, () -> InferenceEndpointRegistry.setInstance(null));
    }
}
