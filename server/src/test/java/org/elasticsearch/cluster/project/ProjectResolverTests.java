/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ProjectResolverTests extends ESTestCase {

    private ThreadPool threadPool;
    private Supplier<ProjectId> fallbackProject;
    private Supplier<Boolean> allowAllProjects;
    private AbstractProjectResolver resolver;

    @Before
    public void initialize() {
        threadPool = new TestThreadPool(getClass().getName());
        fallbackProject = () -> {
            fail("fallbackProject not configured");
            return null;
        };
        allowAllProjects = () -> {
            fail("allowAllProjects not configured");
            return false;
        };
        this.resolver = new AbstractProjectResolver(() -> threadPool.getThreadContext()) {
            @Override
            protected ProjectId getFallbackProjectId() {
                return fallbackProject.get();
            }

            @Override
            protected boolean allowAccessToAllProjects(ThreadContext threadContext) {
                return allowAllProjects.get();
            }
        };
    }

    @After
    public void cleanup() {
        terminate(threadPool);
    }

    public void testGetById() {
        var projects = createProjects();
        var expectedProject = ProjectMetadata.builder(randomUniqueProjectId()).build();
        projects.put(expectedProject.id(), expectedProject);
        var metadata = Metadata.builder().projectMetadata(projects).build();
        threadPool.getThreadContext().putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, expectedProject.id().id());
        var actualProject = resolver.getProjectMetadata(metadata);
        // Ideally, we'd want to use `assertSame` on the projects themselves, but because we're currently still "re-building" projects in
        // Metadata.Builder, the instances won't be exactly the same.
        assertEquals(expectedProject.id(), actualProject.id());
    }

    public void testFallbackToDefaultProject() {
        fallbackProject = () -> Metadata.DEFAULT_PROJECT_ID;
        var projects = createProjects();
        var expectedProject = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID).build();
        projects.put(expectedProject.id(), expectedProject);
        var metadata = Metadata.builder().projectMetadata(projects).build();
        var actualProject = resolver.getProjectMetadata(metadata);
        // Ideally, we'd want to use `assertSame` on the projects themselves, but because we're currently still "re-building" projects in
        // Metadata.Builder, the instances won't be exactly the same.
        assertEquals(expectedProject.id(), actualProject.id());
    }

    public void testFallbackToException() {
        fallbackProject = () -> { throw new UnsupportedOperationException("No fallback allowed"); };
        var projects = createProjects();
        var expectedProject = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID).build();
        projects.put(expectedProject.id(), expectedProject);
        var metadata = Metadata.builder().projectMetadata(projects).build();
        final UnsupportedOperationException ex = expectThrows(
            UnsupportedOperationException.class,
            () -> resolver.getProjectMetadata(metadata)
        );
        assertThat(ex.getMessage(), equalTo("No fallback allowed"));
    }

    public void testGetByIdNonExisting() {
        var projects = createProjects();
        var metadata = Metadata.builder().projectMetadata(projects).build();
        threadPool.getThreadContext().putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, randomUUID());
        assertThrows(IllegalArgumentException.class, () -> resolver.getProjectMetadata(metadata));
    }

    public void testGetAllProjectIdsWhenAllowed() {
        allowAllProjects = () -> true;
        var projects = createProjects();
        var randomProject = ProjectMetadata.builder(randomUniqueProjectId()).build();
        projects.put(randomProject.id(), randomProject);
        var state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().projectMetadata(projects).build()).build();
        var actualProjects = resolver.getProjectIds(state);
        assertEquals(projects.size(), actualProjects.size());
        for (ProjectId projectId : projects.keySet()) {
            assertTrue(actualProjects.contains(projectId));
        }
    }

    public void testGetAllProjectIdsWhenNotAllowed() {
        allowAllProjects = () -> false;
        var projects = createProjects();
        var state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().projectMetadata(projects).build()).build();
        final ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class, () -> resolver.getProjectIds(state));
        assertThat(ex.getMessage(), Matchers.equalTo("No project id supplied, and not permitted to access all projects"));
    }

    public void testGetProjectIdsWithHeader() {
        var projects = createProjects();
        var expectedProject = ProjectMetadata.builder(randomUniqueProjectId()).build();
        projects.put(expectedProject.id(), expectedProject);
        var state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().projectMetadata(projects).build()).build();
        threadPool.getThreadContext().putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, expectedProject.id().id());
        var actualProjects = resolver.getProjectIds(state);
        assertEquals(1, actualProjects.size());
        assertEquals(expectedProject.id(), actualProjects.iterator().next());
    }

    public void testExecuteOnProject() {
        final ProjectId projectId1 = randomUniqueProjectId();
        final ProjectId projectId2 = randomUniqueProjectId();

        final Map<ProjectId, ProjectMetadata> projects = createProjects();
        projects.put(projectId1, ProjectMetadata.builder(projectId1).build());
        projects.put(projectId2, ProjectMetadata.builder(projectId2).build());

        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().projectMetadata(projects)).build();

        final ThreadContext threadContext = threadPool.getThreadContext();

        final String opaqueId = randomAlphaOfLengthBetween(4, 8);
        threadContext.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId);

        final String randomHeaderName = randomAlphaOfLength(10);
        final String randomHeaderValue = randomAlphaOfLength(16);
        threadContext.putHeader(randomHeaderName, randomHeaderValue);

        // Check that no project header was set
        allowAllProjects = () -> true;
        assertThat(resolver.getProjectIds(state), equalTo(projects.keySet()));

        assertThat(threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER), equalTo(opaqueId));
        assertThat(threadContext.getHeader(randomHeaderName), equalTo(randomHeaderValue));

        resolver.executeOnProject(projectId1, () -> {
            assertThat(resolver.getProjectMetadata(state).id(), equalTo(projectId1));
            assertThat(resolver.getProjectId(), equalTo(projectId1));
            assertThat(threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER), equalTo(opaqueId));
            assertThat(threadContext.getHeader(randomHeaderName), equalTo(randomHeaderValue));

            // Cannot change the project-id
            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> resolver.executeOnProject(projectId2, () -> {}));
            assertThat(ex.getMessage(), containsString("project-id [" + projectId1 + "] in the thread-context"));
            assertThat(ex.getMessage(), containsString("[" + projectId2 + "]"));

            // Also cannot set it to itself (this is almost certainly an error, and we prevent it
            ex = expectThrows(IllegalStateException.class, () -> resolver.executeOnProject(projectId1, () -> {}));
            assertThat(ex.getMessage(), containsString("project-id [" + projectId1 + "] in the thread-context"));
        });

        // Project id has been cleared
        assertThat(resolver.getProjectIds(state), equalTo(projects.keySet()));
        assertThat(threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER), equalTo(opaqueId));
        assertThat(threadContext.getHeader(randomHeaderName), equalTo(randomHeaderValue));

        // Can set a new project id, after the previous one has been cleared
        resolver.executeOnProject(projectId2, () -> {
            assertThat(resolver.getProjectMetadata(state).id(), equalTo(projectId2));
            assertThat(resolver.getProjectId(), equalTo(projectId2));
            assertThat(threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER), equalTo(opaqueId));
            assertThat(threadContext.getHeader(randomHeaderName), equalTo(randomHeaderValue));

            // Cannot change the project-id
            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> resolver.executeOnProject(projectId1, () -> {}));
            assertThat(ex.getMessage(), containsString("project-id [" + projectId2 + "] in the thread-context"));
            assertThat(ex.getMessage(), containsString("[" + projectId2 + "]"));
        });

        assertThat(resolver.getProjectIds(state), equalTo(projects.keySet()));
        assertThat(threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER), equalTo(opaqueId));
        assertThat(threadContext.getHeader(randomHeaderName), equalTo(randomHeaderValue));
    }

    public void testShouldSupportsMultipleProjects() {
        assertThat(resolver.supportsMultipleProjects(), equalTo(true));
    }

    private static Map<ProjectId, ProjectMetadata> createProjects() {
        return randomMap(0, 5, () -> {
            var id = randomUniqueProjectId();
            return Tuple.tuple(id, ProjectMetadata.builder(id).build());
        });
    }
}
