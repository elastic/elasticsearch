/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.multiproject.action.DeleteProjectAction.RemoveProjectTask;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.stream.Stream;

public class DeleteProjectActionTests extends ESTestCase {

    private DeleteProjectAction.RemoveProjectExecutor executor;

    @Before
    public void init() {
        executor = new DeleteProjectAction.RemoveProjectExecutor();
    }

    public void testSimpleDelete() throws Exception {
        var projects = randomList(1, 5, ESTestCase::randomUniqueProjectId);
        var deletedProjects = randomSubsetOf(projects);
        var state = buildState(projects);
        var tasks = deletedProjects.stream().map(this::createTask).toList();
        var result = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(state, executor, tasks);
        for (ProjectId deletedProject : deletedProjects) {
            assertNull(result.metadata().projects().get(deletedProject));
            assertNull(result.globalRoutingTable().routingTables().get(deletedProject));
        }
        for (ProjectId project : projects) {
            if (deletedProjects.contains(project)) {
                continue;
            }
            assertNotNull(result.metadata().projects().get(project));
            assertNotNull(result.globalRoutingTable().routingTables().get(project));
        }
    }

    public void testDeleteNonExisting() throws Exception {
        var projects = randomList(1, 5, ESTestCase::randomUniqueProjectId);
        var deletedProjects = randomSubsetOf(projects);
        var state = buildState(projects);
        var listener = ActionListener.<Void>assertAtLeastOnce(
            ActionTestUtils.assertNoSuccessListener(e -> assertTrue(e instanceof IllegalArgumentException))
        );
        var nonExistingTask = createTask(randomUniqueProjectId(), listener);
        var tasks = Stream.concat(Stream.of(nonExistingTask), deletedProjects.stream().map(this::createTask)).toList();
        var result = ClusterStateTaskExecutorUtils.executeHandlingResults(state, executor, tasks, t -> {}, RemoveProjectTask::onFailure);
        for (ProjectId deletedProject : deletedProjects) {
            assertNull(result.metadata().projects().get(deletedProject));
            assertNull(result.globalRoutingTable().routingTables().get(deletedProject));
        }
        for (ProjectId project : projects) {
            if (deletedProjects.contains(project)) {
                continue;
            }
            assertNotNull(result.metadata().projects().get(project));
            assertNotNull(result.globalRoutingTable().routingTables().get(project));
        }
    }

    private RemoveProjectTask createTask(ProjectId projectId) {
        return createTask(projectId, ActionListener.running(() -> {}));
    }

    private RemoveProjectTask createTask(ProjectId projectId, ActionListener<Void> listener) {
        return new RemoveProjectTask(projectId, listener);
    }

    private ClusterState buildState(List<ProjectId> projects) {
        var metadata = Metadata.builder();
        var routingTable = GlobalRoutingTable.builder();
        for (ProjectId projectId : projects) {
            metadata.put(ProjectMetadata.builder(projectId).build());
            routingTable.put(projectId, RoutingTable.EMPTY_ROUTING_TABLE);
        }
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata.build()).routingTable(routingTable.build()).build();
    }
}
