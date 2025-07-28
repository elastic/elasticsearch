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
import static org.hamcrest.Matchers.is;
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
        final ProjectId projectId = randomUniqueProjectId();
        final ProjectResolver projectResolver = TestProjectResolvers.singleProject(projectId);
        assertThat(projectResolver.supportsMultipleProjects(), is(true));
        assertThat(projectResolver.getProjectId(), equalTo(projectId));

        ClusterState state = buildClusterState(projectId, randomIntBetween(0, 10));
        assertThat(projectResolver.getProjectMetadata(state), notNullValue());
    }

    public void testAlwaysThrowProjectResolver() {
        final ProjectResolver projectResolver = TestProjectResolvers.alwaysThrow();
        expectThrows(UnsupportedOperationException.class, projectResolver::getProjectId);
        expectThrows(UnsupportedOperationException.class, projectResolver::supportsMultipleProjects);
        expectThrows(UnsupportedOperationException.class, () -> projectResolver.executeOnProject(randomProjectIdOrDefault(), () -> {}));
        expectThrows(
            UnsupportedOperationException.class,
            () -> projectResolver.getProjectMetadata(buildClusterState(randomProjectIdOrDefault(), randomIntBetween(0, 10)))
        );
    }

    public void testDefaultProjectOnly() {
        final ProjectResolver projectResolver = TestProjectResolvers.DEFAULT_PROJECT_ONLY;
        assertThat(projectResolver.supportsMultipleProjects(), is(false));
        assertThat(projectResolver.getProjectId(), equalTo(ProjectId.DEFAULT));

        ClusterState state = buildClusterState(ProjectId.DEFAULT, 0);
        assertThat(projectResolver.getProjectMetadata(state), notNullValue());
    }

    public void testMustExecuteFirst_getProjectIdAndMetadata() {
        final ProjectId projectId = randomUniqueProjectId();
        final ClusterState state = buildClusterState(projectId);

        final ProjectResolver projectResolver = TestProjectResolvers.mustExecuteFirst();
        expectThrows(UnsupportedOperationException.class, projectResolver::getProjectId);
        expectThrows(UnsupportedOperationException.class, () -> projectResolver.getProjectMetadata(state));

        projectResolver.executeOnProject(projectId, () -> {
            assertThat(projectResolver.getProjectId(), equalTo(projectId));
            assertThat(projectResolver.getProjectMetadata(state), equalTo(state.metadata().getProject(projectId)));
        });
    }

    public void testMustExecuteFirst_getProjectIds() {
        {
            final ProjectResolver projectResolver = TestProjectResolvers.mustExecuteFirst();
            final ProjectId projectId = randomUniqueProjectId();
            ClusterState state = buildClusterState(projectId);
            assertThat(state.metadata().projects().values(), hasSize(1));

            expectThrows(UnsupportedOperationException.class, () -> projectResolver.getProjectIds(state));
            projectResolver.executeOnProject(projectId, () -> assertThat(projectResolver.getProjectIds(state), contains(projectId)));
            projectResolver.executeOnProject(randomUniqueProjectId(), () -> {
                expectThrows(IllegalArgumentException.class, () -> projectResolver.getProjectIds(state));
            });
        }
        {
            final ProjectResolver projectResolver = TestProjectResolvers.mustExecuteFirst();
            final ProjectId projectId = randomUniqueProjectId();
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
            metadata.put(ProjectMetadata.builder(ProjectId.fromId("p" + i + "_" + randomAlphaOfLength(8))).build());
        }
        return ClusterState.builder(new ClusterName(randomAlphaOfLengthBetween(4, 8))).metadata(metadata).build();
    }

    private ClusterState buildClusterState(int numberOfProjects) {
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 0; i < numberOfProjects; i++) {
            metadata.put(ProjectMetadata.builder(ProjectId.fromId("p" + i + "_" + randomAlphaOfLength(8))).build());
        }
        return ClusterState.builder(new ClusterName(randomAlphaOfLengthBetween(4, 8))).metadata(metadata).build();
    }

}
