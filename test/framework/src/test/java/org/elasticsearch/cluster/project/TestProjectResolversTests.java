/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.notNullValue;

public class TestProjectResolversTests extends ESTestCase {

    public void testAllProjects_getProjectMetadata() {
        {
            ClusterState state = buildClusterState(1);
            assertThat(state.metadata().projects().values(), hasSize(1));

            ProjectMetadata project = TestProjectResolvers.allProjects().getProjectMetadata(state);
            assertThat(project, notNullValue());
            assertThat(project, in(state.metadata().projects().values()));
        }
        {
            ClusterState state = buildClusterState(randomIntBetween(2, 10));
            assertThat(state.metadata().projects().values().size(), greaterThan(1));

            expectThrows(IllegalStateException.class, () -> TestProjectResolvers.allProjects().getProjectMetadata(state));
        }
    }

    public void testAllProjects_getProjectId() {
        {
            ClusterState state = buildClusterState(1);
            assertThat(state.metadata().projects().values(), hasSize(1));

            ProjectId project = TestProjectResolvers.allProjects().getProjectId(state);
            assertThat(project, notNullValue());
            assertThat(project, in(state.metadata().projects().keySet()));
        }
        {
            ClusterState state = buildClusterState(randomIntBetween(2, 10));
            assertThat(state.metadata().projects().values().size(), greaterThan(1));

            expectThrows(IllegalStateException.class, () -> TestProjectResolvers.allProjects().getProjectId(state));
        }
    }

    public void testAllProjects_getProjectIds() {
        for (int numberOfProjects : List.of(1, 3, 8)) {
            ClusterState state = buildClusterState(numberOfProjects);
            assertThat(state.metadata().projects().values(), hasSize(numberOfProjects));

            Collection<ProjectId> projects = TestProjectResolvers.allProjects().getProjectIds(state);
            assertThat(projects, notNullValue());
            assertThat(projects, hasSize(numberOfProjects));
            assertThat(projects, equalTo(state.metadata().projects().keySet()));
        }
    }

    public void testSingleProjectOnly_getProjectMetadata() {
        {
            ClusterState state = buildClusterState(1);
            assertThat(state.metadata().projects().values(), hasSize(1));

            ProjectMetadata project = TestProjectResolvers.singleProjectOnly().getProjectMetadata(state);
            assertThat(project, notNullValue());
            assertThat(project, in(state.metadata().projects().values()));
        }
        {
            ClusterState state = buildClusterState(randomIntBetween(2, 10));
            assertThat(state.metadata().projects().values().size(), greaterThan(1));

            expectThrows(IllegalStateException.class, () -> TestProjectResolvers.singleProjectOnly().getProjectMetadata(state));
        }
    }

    public void testSingleProjectOnly_getProjectId() {
        {
            ClusterState state = buildClusterState(1);
            assertThat(state.metadata().projects().values(), hasSize(1));

            ProjectId project = TestProjectResolvers.singleProjectOnly().getProjectId(state);
            assertThat(project, notNullValue());
            assertThat(project, in(state.metadata().projects().keySet()));
        }
        {
            ClusterState state = buildClusterState(randomIntBetween(2, 10));
            assertThat(state.metadata().projects().values().size(), greaterThan(1));

            expectThrows(IllegalStateException.class, () -> TestProjectResolvers.singleProjectOnly().getProjectId(state));
        }
    }

    public void testSingleProjectOnly_getProjectIds() {
        {
            ClusterState state = buildClusterState(1);
            assertThat(state.metadata().projects().values(), hasSize(1));

            Collection<ProjectId> projects = TestProjectResolvers.singleProjectOnly().getProjectIds(state);
            assertThat(projects, notNullValue());
            assertThat(projects, hasSize(1));
            assertThat(projects, equalTo(state.metadata().projects().keySet()));
        }
        {
            ClusterState state = buildClusterState(randomIntBetween(2, 10));
            assertThat(state.metadata().projects().values().size(), greaterThan(1));

            expectThrows(IllegalStateException.class, () -> TestProjectResolvers.singleProjectOnly().getProjectIds(state));
        }
    }

    public void testSpecifiedProjects_getProjectMetadata() {
        for (int numberOfProjects : List.of(1, 3, 8)) {
            ClusterState state = buildClusterState(numberOfProjects);
            assertThat(state.metadata().projects().values(), hasSize(numberOfProjects));
            final Set<ProjectId> allIds = state.metadata().projects().keySet();

            final ProjectId id = randomFrom(allIds);
            ProjectMetadata project = TestProjectResolvers.projects(Set.of(id)).getProjectMetadata(state);
            assertThat(project, notNullValue());
            assertThat(project, in(state.metadata().projects().values()));
            assertThat(project.id(), equalTo(id));

            final ProjectId other = randomValueOtherThanMany(allIds::contains, () -> new ProjectId(randomUUID()));
            expectThrows(IllegalStateException.class, () -> TestProjectResolvers.projects(Set.of(other)).getProjectMetadata(state));

            project = TestProjectResolvers.projects(Set.of(id, other)).getProjectMetadata(state);
            assertThat(project, notNullValue());
            assertThat(project, in(state.metadata().projects().values()));
            assertThat(project.id(), equalTo(id));

            if (numberOfProjects > 1) {
                expectThrows(IllegalStateException.class, () -> TestProjectResolvers.projects(allIds).getProjectMetadata(state));
            }
        }
    }

    public void testSpecifiedProjects_getProjectId() {
        for (int numberOfProjects : List.of(1, 3, 8)) {
            ClusterState state = buildClusterState(numberOfProjects);
            assertThat(state.metadata().projects().values(), hasSize(numberOfProjects));
            final Set<ProjectId> allIds = state.metadata().projects().keySet();

            final ProjectId id = randomFrom(allIds);
            ProjectId project = TestProjectResolvers.projects(Set.of(id)).getProjectId(state);
            assertThat(project, notNullValue());
            assertThat(project, equalTo(id));

            final ProjectId other = randomValueOtherThanMany(allIds::contains, () -> new ProjectId(randomUUID()));
            expectThrows(IllegalStateException.class, () -> TestProjectResolvers.projects(Set.of(other)).getProjectId(state));

            project = TestProjectResolvers.projects(Set.of(id, other)).getProjectId(state);
            assertThat(project, notNullValue());
            assertThat(project, equalTo(id));

            if (numberOfProjects > 1) {
                expectThrows(IllegalStateException.class, () -> TestProjectResolvers.projects(allIds).getProjectId(state));
            }
        }
    }

    public void testSpecifiedProjects_getProjectIds() {
        for (int numberOfProjects : List.of(1, 3, 8)) {
            ClusterState state = buildClusterState(numberOfProjects);
            assertThat(state.metadata().projects().values(), hasSize(numberOfProjects));
            final Set<ProjectId> allIds = state.metadata().projects().keySet();

            Collection<ProjectId> projects = TestProjectResolvers.projects(allIds).getProjectIds(state);
            assertThat(projects, notNullValue());
            assertThat(projects, equalTo(allIds));

            final ProjectId id = randomFrom(allIds);
            projects = TestProjectResolvers.projects(Set.of(id)).getProjectIds(state);
            assertThat(projects, notNullValue());
            assertThat(projects, hasSize(1));
            assertThat(projects, contains(id));

            final ProjectId other = randomValueOtherThanMany(allIds::contains, () -> new ProjectId(randomUUID()));
            assertThat(TestProjectResolvers.projects(Set.of(other)).getProjectIds(state), empty());

            projects = TestProjectResolvers.projects(Set.of(id, other)).getProjectIds(state);
            assertThat(projects, notNullValue());
            assertThat(projects, hasSize(1));
            assertThat(projects, contains(id));
        }
    }

    private ClusterState buildClusterState(int numberOfProjects) {
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 0; i < numberOfProjects; i++) {
            metadata.put(ProjectMetadata.builder(new ProjectId("p" + i + "_" + randomAlphaOfLength(8))).build());
        }
        return ClusterState.builder(new ClusterName(randomAlphaOfLengthBetween(4, 8))).metadata(metadata).build();
    }

}
