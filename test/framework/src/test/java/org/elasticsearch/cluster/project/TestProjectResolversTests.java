/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class TestProjectResolversTests extends ESTestCase {

    public void testAllProjects() {
        final int numberOfProjects = randomIntBetween(1, 10);
        ClusterState state = buildClusterState(numberOfProjects);
        assertThat(state.metadata().projects().values(), hasSize(numberOfProjects));

        expectThrows(UnsupportedOperationException.class, () -> TestProjectResolvers.allProjects().getProjectMetadata(state));
        expectThrows(UnsupportedOperationException.class, () -> TestProjectResolvers.allProjects().getProjectId());
        assertThat(TestProjectResolvers.allProjects().getProjectIds(state), equalTo(state.metadata().projects().keySet()));
    }

    public void testSingleProject() {
        final ProjectId projectId = new ProjectId(randomUUID());
        final ProjectResolver projectResolver = TestProjectResolvers.singleProject(projectId);
        assertThat(projectResolver.getProjectId(), equalTo(projectId));

        ClusterState state = buildClusterState(projectId, randomIntBetween(0, 10));
        assertThat(projectResolver.getProjectMetadata(state), notNullValue());
    }

    public void testSingleProjectOnly_getProjectIdAndMetadata() {
        final ProjectId projectId = new ProjectId(randomUUID());
        final ClusterState state = buildClusterState(projectId);

        final ProjectResolver projectResolver = TestProjectResolvers.singleProjectOnly();
        expectThrows(UnsupportedOperationException.class, projectResolver::getProjectId);
        expectThrows(UnsupportedOperationException.class, () -> projectResolver.getProjectMetadata(state));

        projectResolver.executeOnProject(projectId, () -> {
            assertThat(projectResolver.getProjectId(), equalTo(projectId));
            assertThat(projectResolver.getProjectMetadata(state), equalTo(state.metadata().getProject(projectId)));
        });
    }

    public void testSingleProjectOnly_getProjectIds() {
        {
            final ProjectResolver projectResolver = TestProjectResolvers.singleProjectOnly();
            final ProjectId projectId = new ProjectId(randomUUID());
            ClusterState state = buildClusterState(projectId);
            assertThat(state.metadata().projects().values(), hasSize(1));

            expectThrows(UnsupportedOperationException.class, () -> projectResolver.getProjectIds(state));
            projectResolver.executeOnProject(projectId, () -> assertThat(projectResolver.getProjectIds(state), contains(projectId)));
            projectResolver.executeOnProject(new ProjectId(randomUUID()), () -> {
                expectThrows(IllegalArgumentException.class, () -> projectResolver.getProjectIds(state));
            });
        }
        {
            final ProjectResolver projectResolver = TestProjectResolvers.singleProjectOnly();
            final ProjectId projectId = new ProjectId(randomUUID());
            ClusterState state = buildClusterState(projectId, randomIntBetween(1, 10));
            assertThat(state.metadata().projects().values().size(), greaterThan(1));

            projectResolver.executeOnProject(
                projectId,
                () -> expectThrows(IllegalStateException.class, () -> projectResolver.getProjectIds(state))
            );
        }
    }

    private ClusterState buildClusterState(ProjectId... projectIds) {
        Metadata.Builder metadata = Metadata.builder();
        for (var projectId : projectIds) {
            metadata.put(ProjectMetadata.builder(projectId).build());
        }
        return ClusterState.builder(new ClusterName(randomAlphaOfLengthBetween(4, 8))).metadata(metadata).build();
    }

    private ClusterState buildClusterState(ProjectId projectId, int numberOfExtraProjects) {
        Metadata.Builder metadata = Metadata.builder();
        metadata.put(ProjectMetadata.builder(projectId).build());
        for (int i = 0; i < numberOfExtraProjects; i++) {
            metadata.put(ProjectMetadata.builder(new ProjectId("p" + i + "_" + randomAlphaOfLength(8))).build());
        }
        return ClusterState.builder(new ClusterName(randomAlphaOfLengthBetween(4, 8))).metadata(metadata).build();
    }

    private ClusterState buildClusterState(int numberOfProjects) {
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 0; i < numberOfProjects; i++) {
            metadata.put(ProjectMetadata.builder(new ProjectId("p" + i + "_" + randomAlphaOfLength(8))).build());
        }
        return ClusterState.builder(new ClusterName(randomAlphaOfLengthBetween(4, 8))).metadata(metadata).build();
    }

}
