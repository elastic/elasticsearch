/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ProjectDeletedListenerTests extends ESTestCase {

    public void testInvocation() {
        final Set<ProjectId> projects = new HashSet<>();
        var pdl = new ProjectDeletedListener(projects::add);
        var cs = mock(ClusterService.class);

        final AtomicReference<ClusterStateListener> csl = new AtomicReference<>();
        doAnswer(inv -> {
            assertThat(inv.getArguments(), arrayWithSize(1));
            csl.set(inv.getArgument(0));
            return null;
        }).when(cs).addListener(any(ClusterStateListener.class));

        pdl.attach(cs);

        assertThat(csl.get(), notNullValue());

        final List<ProjectId> existingProjects = randomList(5, 15, ESTestCase::randomUniqueProjectId);

        final ClusterState.Builder csBuilder = ClusterState.builder(ClusterName.DEFAULT);
        existingProjects.forEach(p -> csBuilder.putProjectMetadata(ProjectMetadata.builder(p).build()));
        final ClusterState cs0 = csBuilder.build();

        final Set<ProjectId> projectsToDelete = Set.copyOf(
            randomSubsetOf(randomIntBetween(1, existingProjects.size() / 2), existingProjects)
        );
        final List<ProjectId> projectsToCreate = randomList(0, 3, ESTestCase::randomUniqueProjectId);
        final var mdBuilder = Metadata.builder(cs0.metadata());
        projectsToDelete.forEach(mdBuilder::removeProject);
        projectsToCreate.forEach(p -> mdBuilder.put(ProjectMetadata.builder(p).build()));
        var md = mdBuilder.build();
        var cs1 = ClusterState.builder(cs0)
            .metadata(md)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(md, RoutingTable.Builder::addAsNew))
            .build();

        csl.get().clusterChanged(new ClusterChangedEvent(getTestName(), cs1, cs0));
        assertThat(projects, equalTo(projectsToDelete));
    }

}
