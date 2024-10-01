/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiProjectResolverTests extends ESTestCase {

    private ThreadPool threadPool;
    private MultiProjectResolver resolver;

    @Before
    public void initialize() {
        threadPool = new TestThreadPool(getClass().getName());
        var plugin = new MultiProjectPlugin();
        var pluginServices = mock(Plugin.PluginServices.class);
        when(pluginServices.threadPool()).thenReturn(threadPool);
        plugin.createComponents(pluginServices);
        this.resolver = new MultiProjectResolver(plugin);
    }

    @After
    public void cleanup() {
        terminate(threadPool);
    }

    public void testGetById() {
        var projects = randomMap(0, 5, () -> {
            var id = new ProjectId(randomUUID());
            return Tuple.tuple(id, ProjectMetadata.builder(id).build());
        });
        var expectedProject = ProjectMetadata.builder(new ProjectId(randomUUID())).build();
        projects.put(expectedProject.id(), expectedProject);
        var metadata = Metadata.builder().projectMetadata(projects).build();
        threadPool.getThreadContext().putHeader(MultiProjectPlugin.PROJECT_ID_REST_HEADER, expectedProject.id().id());
        var actualProject = resolver.getProjectMetadata(metadata);
        // Ideally, we'd want to use `assertSame` on the projects themselves, but because we're currently still "re-building" projects in
        // Metadata.Builder, the instances won't be exactly the same.
        assertEquals(expectedProject.id(), actualProject.id());
    }

    public void testFallback() {
        var projects = randomMap(0, 5, () -> {
            var id = new ProjectId(randomUUID());
            return Tuple.tuple(id, ProjectMetadata.builder(id).build());
        });
        var expectedProject = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID).build();
        projects.put(expectedProject.id(), expectedProject);
        var metadata = Metadata.builder().projectMetadata(projects).build();
        var actualProject = resolver.getProjectMetadata(metadata);
        // Ideally, we'd want to use `assertSame` on the projects themselves, but because we're currently still "re-building" projects in
        // Metadata.Builder, the instances won't be exactly the same.
        assertEquals(expectedProject.id(), actualProject.id());
    }

    public void testGetByIdNonExisting() {
        var projects = randomMap(0, 5, () -> {
            var id = new ProjectId(randomUUID());
            return Tuple.tuple(id, ProjectMetadata.builder(id).build());
        });
        var metadata = Metadata.builder().projectMetadata(projects).build();
        threadPool.getThreadContext().putHeader(MultiProjectPlugin.PROJECT_ID_REST_HEADER, randomUUID());
        assertThrows(IllegalArgumentException.class, () -> resolver.getProjectMetadata(metadata));
    }
}
