/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

public class TransportClusterStateActionTests extends ESTestCase {

    private TestThreadPool threadPool;
    private TransportService transportService;
    private Task task;
    private IndexNameExpressionResolver indexResolver;

    @Before
    public void start() {
        threadPool = new TestThreadPool(getTestName());
        transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        transportService.start();
        indexResolver = TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext());
        task = new CancellableTask(
            randomLong(),
            randomAlphaOfLength(8),
            ClusterStateAction.NAME,
            randomAlphaOfLengthBetween(8, 24),
            null,
            Map.of()
        );
    }

    @After
    public void stop() {
        transportService.stop();
        threadPool.close();
    }

    public void testGetClusterStateWithDefaultProjectOnly() throws Exception {
        final ProjectResolver projectResolver = DefaultProjectResolver.INSTANCE;

        final Set<String> indexNames = randomSet(1, 8, () -> randomAlphaOfLengthBetween(4, 12));
        final ClusterStateRequest request = buildRandomRequest(indexNames);
        final String[] expectedIndices = getExpectedIndices(request, indexNames);

        final ProjectId projectId = Metadata.DEFAULT_PROJECT_ID;
        final ProjectMetadata.Builder project = projectBuilder(projectId, indexNames);
        final ClusterState state = buildClusterState(project);

        final ClusterStateResponse response = executeAction(projectResolver, request, state);

        assertThat(response.getClusterName(), equalTo(state.getClusterName()));
        assertSingleProjectResponse(request, response, projectId, expectedIndices);
    }

    public void testGetClusterStateForOneProjectOfMany() throws Exception {
        final Set<String> indexNames = randomSet(1, 8, () -> randomAlphaOfLengthBetween(4, 12));
        final ProjectId projectId = randomUniqueProjectId();

        final ProjectResolver projectResolver = TestProjectResolvers.singleProject(projectId);
        final ClusterStateRequest request = buildRandomRequest(indexNames);
        final String[] expectedIndices = getExpectedIndices(request, indexNames);

        final int numberOfProjects = randomIntBetween(2, 5);
        final ProjectMetadata.Builder[] projects = new ProjectMetadata.Builder[numberOfProjects];
        projects[0] = projectBuilder(projectId, indexNames);
        for (int i = 1; i < numberOfProjects; i++) {
            projects[i] = projectBuilder(randomUniqueProjectId(), randomSet(0, 12, () -> randomAlphaOfLengthBetween(4, 12)));
        }
        final ClusterState state = buildClusterState(projects);

        final ClusterStateResponse response = executeAction(projectResolver, request, state);

        assertThat(response.getClusterName(), equalTo(state.getClusterName()));
        assertSingleProjectResponse(request, response, projectId, expectedIndices);
    }

    public void testGetClusterStateForManyProjects() throws Exception {
        final int numberOfProjects = randomIntBetween(2, 5);
        final ProjectMetadata.Builder[] projects = new ProjectMetadata.Builder[numberOfProjects];
        final ProjectId[] projectIds = new ProjectId[numberOfProjects];
        final Set<String> indexNames = randomSet(5, 20, () -> randomAlphaOfLengthBetween(4, 12));
        for (int i = 0; i < numberOfProjects; i++) {
            projectIds[i] = randomUniqueProjectId();
            projects[i] = projectBuilder(projectIds[i], randomSubsetOf(indexNames));
        }
        final ClusterState state = buildClusterState(projects);

        final ProjectResolver projectResolver = TestProjectResolvers.allProjects();
        final ClusterStateRequest request = buildRandomRequest(indexNames);
        final Set<String> requestedIndices = Set.of(getExpectedIndices(request, indexNames));

        final ClusterStateResponse response = executeAction(projectResolver, request, state);
        assertThat(response.getClusterName(), equalTo(state.getClusterName()));

        assertThat(response.getState().metadata().projects(), aMapWithSize(numberOfProjects));
        assertThat(response.getState().metadata().projects().keySet(), containsInAnyOrder(projectIds));

        // This is because the ClusterState.Builder currently forces the routing table to contain all projects
        // If we relax that, then it would be OK for this map to be empty when request.routingTable() is false
        assertThat(response.getState().globalRoutingTable().routingTables(), aMapWithSize(numberOfProjects));
        assertThat(response.getState().globalRoutingTable().routingTables().keySet(), containsInAnyOrder(projectIds));

        for (ProjectId projectId : projectIds) {
            String[] expectedIndices = Sets.intersection(state.metadata().getProject(projectId).indices().keySet(), requestedIndices)
                .toArray(String[]::new);
            final ProjectMetadata project = response.getState().metadata().getProject(projectId);
            assertThat(project, notNullValue());
            if (request.metadata()) {
                assertThat(project.indices().keySet(), containsInAnyOrder(expectedIndices));
            } else {
                assertThat(project.indices(), anEmptyMap());
            }
            if (request.routingTable()) {
                final RoutingTable routingTable = response.getState().globalRoutingTable().routingTable(projectId);
                assertThat(routingTable, notNullValue());
                assertThat(routingTable.indicesRouting().keySet(), containsInAnyOrder(expectedIndices));
            }
        }
    }

    private static void assertSingleProjectResponse(
        ClusterStateRequest request,
        ClusterStateResponse response,
        ProjectId projectId,
        String[] expectedIndices
    ) {
        final Metadata metadata = response.getState().metadata();
        assertThat(metadata.projects().keySet(), contains(projectId));
        if (request.metadata()) {
            assertThat(metadata.getProject(projectId).indices().keySet(), containsInAnyOrder(expectedIndices));
        } else {
            assertThat(metadata.getProject(projectId).indices(), anEmptyMap());
        }

        final ImmutableOpenMap<ProjectId, RoutingTable> routingTables = response.getState().globalRoutingTable().routingTables();
        assertThat(routingTables, aMapWithSize(1));
        assertThat(routingTables, hasKey(projectId));
        if (request.routingTable()) {
            assertThat(routingTables.get(projectId).indicesRouting().keySet(), containsInAnyOrder(expectedIndices));
        } else {
            assertThat(routingTables.get(projectId).indicesRouting(), anEmptyMap());
        }
    }

    private ClusterStateResponse executeAction(ProjectResolver projectResolver, ClusterStateRequest request, ClusterState state)
        throws IOException, InterruptedException, ExecutionException {
        final TransportClusterStateAction action = new TransportClusterStateAction(
            transportService,
            null,
            threadPool,
            new ActionFilters(Set.of()),
            indexResolver,
            projectResolver
        );
        final PlainActionFuture<ClusterStateResponse> future = new PlainActionFuture<>();
        action.masterOperation(task, request, state, future);
        return future.get();
    }

    private static String[] getExpectedIndices(ClusterStateRequest request, Set<String> indexNames) {
        if (request.indices().length > 0) {
            return request.indices();
        } else {
            return indexNames.toArray(String[]::new);
        }
    }

    private static ClusterStateRequest buildRandomRequest(Set<String> indexNames) {
        final ClusterStateRequest request = new ClusterStateRequest(TEST_REQUEST_TIMEOUT);
        if (randomBoolean()) {
            final int numberSelectedIndices = randomIntBetween(1, indexNames.size());
            request.indices(randomSubsetOf(numberSelectedIndices, indexNames).toArray(String[]::new));
            request.metadata(true);
        } else {
            request.metadata(randomBoolean());
        }
        request.nodes(randomBoolean());
        request.routingTable(randomBoolean());
        request.blocks(randomBoolean());
        request.customs(randomBoolean());
        return request;
    }

    private static ClusterState buildClusterState(ProjectMetadata.Builder... projects) {
        final Metadata.Builder metadataBuilder = Metadata.builder();
        Arrays.stream(projects).forEach(metadataBuilder::put);
        final var metadata = metadataBuilder.build();

        return ClusterState.builder(new ClusterName(randomAlphaOfLengthBetween(4, 12)))
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .build();
    }

    private static ProjectMetadata.Builder projectBuilder(ProjectId projectId, Collection<String> indexNames) {
        final ProjectMetadata.Builder project = ProjectMetadata.builder(projectId);
        indexNames.forEach(
            index -> project.put(
                IndexMetadata.builder(index)
                    .settings(
                        indexSettings(IndexVersion.current(), randomIntBetween(1, 5), randomIntBetween(0, 3)).put(
                            IndexMetadata.SETTING_INDEX_UUID,
                            randomUUID()
                        )
                    )
                    .build(),
                false
            )
        );
        return project;
    }
}
