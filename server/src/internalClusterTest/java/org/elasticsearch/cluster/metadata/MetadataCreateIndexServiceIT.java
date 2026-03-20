/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.multiproject.TestOnlyMultiProjectPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class MetadataCreateIndexServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestOnlyMultiProjectPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TestOnlyMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), true)
            .build();
    }

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    private enum CreateIndexResult {
        SUCCESS,
        INVALID_NAME,
        INVALID_SETTINGS,
        ALREADY_EXISTS
    }

    private record CreateIndexRequestSpec(
        String indexName,
        boolean invalidSettings,
        CreateIndexResult expectedResult,
        ProjectId projectId
    ) {}

    public void testRequestTemplateIsRespected() throws InterruptedException {
        /*
         * This test passes a template in the CreateIndexClusterStateUpdateRequest, and makes sure that the settings
         * from that template are used when creating the index.
         */
        MetadataCreateIndexService metadataCreateIndexService = internalCluster().getCurrentMasterNodeInstance(
            MetadataCreateIndexService.class
        );
        final String indexName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        final int numberOfReplicas = randomIntBetween(1, 7);
        CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "testRequestTemplateIsRespected",
            ProjectId.DEFAULT,
            indexName,
            randomAlphaOfLength(20)
        );
        request.setMatchingTemplate(
            ComposableIndexTemplate.builder()
                .template(Template.builder().settings(Settings.builder().put("index.number_of_replicas", numberOfReplicas)))
                .build()
        );
        final CountDownLatch listenerCalledLatch = new CountDownLatch(1);
        ActionListener<ShardsAcknowledgedResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(ShardsAcknowledgedResponse shardsAcknowledgedResponse) {
                listenerCalledLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e);
                listenerCalledLatch.countDown();
            }
        };

        metadataCreateIndexService.createIndex(
            TimeValue.THIRTY_SECONDS,
            TimeValue.THIRTY_SECONDS,
            TimeValue.THIRTY_SECONDS,
            request,
            listener
        );
        listenerCalledLatch.await(10, TimeUnit.SECONDS);
        GetIndexResponse response = admin().indices()
            .getIndex(new GetIndexRequest(TimeValue.THIRTY_SECONDS).indices(indexName))
            .actionGet();
        Settings settings = response.getSettings().get(indexName);
        assertThat(settings.get("index.number_of_replicas"), equalTo(Integer.toString(numberOfReplicas)));
    }

    public void testCreateIndexBatching() throws Exception {
        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var metadataCreateIndexService = internalCluster().getCurrentMasterNodeInstance(MetadataCreateIndexService.class);

        final var projects = new HashSet<ProjectId>();
        final var projectCount = randomIntBetween(1, 5);
        while (projects.size() < projectCount) {
            projects.add(randomUniqueProjectId());
        }
        addProjectsToClusterState(masterClusterService, projects);

        final var allIndexNames = new HashSet<String>();
        final int totalRequestCount = randomIntBetween(1, 20);
        final int validRequestCount = randomIntBetween(0, totalRequestCount);
        final int invalidRequestCount = totalRequestCount - validRequestCount;

        // The same index name can exist in different projects. Pre-create it in all of them
        final var preExistingIndexName = addRandomIndexNameNoCollision(allIndexNames);
        addIndexToAllProjects(metadataCreateIndexService, preExistingIndexName, projects);

        final var allRequests = new ArrayList<CreateIndexRequestSpec>(totalRequestCount);
        final var validIndicesByProject = new HashMap<ProjectId, Set<String>>(projects.size());
        for (int i = 0; i < validRequestCount; i++) {
            final var projectId = randomFrom(projects);
            final var indexName = addRandomIndexNameNoCollision(allIndexNames);
            validIndicesByProject.putIfAbsent(projectId, new HashSet<>());
            validIndicesByProject.get(projectId).add(indexName);
            allRequests.add(new CreateIndexRequestSpec(indexName, false, CreateIndexResult.SUCCESS, projectId));
        }

        final var invalidSettingsNames = new HashSet<String>();
        for (int i = 0; i < invalidRequestCount; i++) {
            final var projectId = randomFrom(projects);
            switch (randomIntBetween(0, 2)) {
                case 0 -> {
                    final var indexName = randomIdentifier("INVALID_BECAUSE_UPPER_CASE_");
                    allRequests.add(new CreateIndexRequestSpec(indexName, false, CreateIndexResult.INVALID_NAME, projectId));
                }
                case 1 -> {
                    final var indexName = addRandomIndexNameNoCollision(allIndexNames);
                    invalidSettingsNames.add(indexName);
                    allRequests.add(new CreateIndexRequestSpec(indexName, true, CreateIndexResult.INVALID_SETTINGS, projectId));
                }
                default -> allRequests.add(
                    new CreateIndexRequestSpec(preExistingIndexName, false, CreateIndexResult.ALREADY_EXISTS, projectId)
                );
            }
        }
        Collections.shuffle(allRequests, random());
        final ClusterStateListener listener = event -> assertAllIndicesCreatedAtomically(
            event,
            projects,
            validIndicesByProject,
            invalidSettingsNames
        );
        masterClusterService.addListener(listener);

        try {
            final var barrier = new CyclicBarrier(2);
            masterClusterService.submitUnbatchedStateUpdateTask("block", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    safeAwait(barrier);
                    safeAwait(barrier);
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("blocking task failed", e);
                }
            });
            safeAwait(barrier);

            final var responsesLatch = new CountDownLatch(allRequests.size());
            for (final var requestSpec : allRequests) {
                final var settingsBuilder = Settings.builder().put("index.routing.allocation.enable", "none");
                if (requestSpec.invalidSettings()) {
                    settingsBuilder.put("index.version.created", 1);
                }
                final var request = new CreateIndexClusterStateUpdateRequest(
                    "create-index",
                    requestSpec.projectId(),
                    requestSpec.indexName(),
                    randomAlphaOfLength(20)
                ).settings(settingsBuilder.build()).waitForActiveShards(ActiveShardCount.NONE);
                metadataCreateIndexService.createIndex(
                    TimeValue.THIRTY_SECONDS,
                    TimeValue.THIRTY_SECONDS,
                    TimeValue.THIRTY_SECONDS,
                    request,
                    new LatchedActionListener<>(listenerForCreateIndexResult(requestSpec.expectedResult()), responsesLatch)
                );
            }

            assertTrue(
                "create-index tasks should be queued behind the blocking task",
                waitUntil(
                    () -> masterClusterService.getMasterService()
                        .pendingTasks()
                        .stream()
                        .filter(pct -> pct.getSource().toString().startsWith("create-index"))
                        .count() == totalRequestCount
                )
            );
            final var initialState = masterClusterService.state();
            safeAwait(barrier);

            assertTrue("timed out waiting for create-index responses", responsesLatch.await(30, TimeUnit.SECONDS));
            if (validIndicesByProject.isEmpty()) {
                assertSame("cluster state should not change when all requests failed", masterClusterService.state(), initialState);
            }
        } finally {
            masterClusterService.removeListener(listener);
        }
    }

    private void assertAllIndicesCreatedAtomically(
        ClusterChangedEvent event,
        Set<ProjectId> projects,
        Map<ProjectId, Set<String>> indicesPerProject,
        Set<String> invalidIndices
    ) {
        var projectsWithValidIndices = 0;
        var allCreatedInProject = 0;
        for (var projectId : projects) {
            final var projectMetadata = event.state().metadata().getProject(projectId);
            final var expectedIndicesInProject = indicesPerProject.getOrDefault(projectId, Set.of());
            final var createdInProject = expectedIndicesInProject.stream().filter(projectMetadata::hasIndex).toList();
            assertThat(
                "expected either none or all valid indices to appear atomically in single project",
                createdInProject,
                anyOf(hasSize(0), hasSize(expectedIndicesInProject.size()))
            );
            if (expectedIndicesInProject.isEmpty() == false) {
                projectsWithValidIndices += 1;
                if (createdInProject.size() == expectedIndicesInProject.size()) {
                    allCreatedInProject += 1;
                }
            }
            invalidIndices.forEach(
                invalid -> assertFalse("invalid index [" + invalid + "] should never be created", projectMetadata.hasIndex(invalid))
            );
        }
        assertThat(
            "expected either none or all valid indices to appear atomically across all projects",
            allCreatedInProject,
            anyOf(equalTo(0), equalTo(projectsWithValidIndices))
        );
    }

    private void addIndexToAllProjects(MetadataCreateIndexService service, String indexName, Set<ProjectId> projects) {
        for (final var projectId : projects) {
            service.createIndex(
                TimeValue.THIRTY_SECONDS,
                TimeValue.THIRTY_SECONDS,
                TimeValue.THIRTY_SECONDS,
                new CreateIndexClusterStateUpdateRequest("create-pre-existing", projectId, indexName, randomAlphaOfLength(20)).settings(
                    Settings.builder().put("index.routing.allocation.enable", "none").build()
                ).waitForActiveShards(ActiveShardCount.NONE),
                assertNoFailureListener(r -> {})
            );
        }
        awaitClusterState(
            state -> projects.stream()
                .allMatch(p -> state.metadata().projects().containsKey(p) && state.metadata().getProject(p).hasIndex(indexName))
        );
    }

    private void addProjectsToClusterState(ClusterService clusterService, Set<ProjectId> projects) {
        clusterService.submitUnbatchedStateUpdateTask("init-projects", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final var metadataBuilder = Metadata.builder(currentState.metadata());
                projects.forEach(p -> metadataBuilder.put(ProjectMetadata.builder(p)));
                return ClusterState.builder(currentState)
                    .metadata(metadataBuilder)
                    .routingTable(currentState.globalRoutingTable().initializeProjects(projects))
                    .build();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("failed to initialize projects", e);
            }
        });
        awaitClusterState(state -> state.metadata().projects().keySet().containsAll(projects));
    }

    private ActionListener<ShardsAcknowledgedResponse> listenerForCreateIndexResult(CreateIndexResult expectedResult) {
        return switch (expectedResult) {
            case SUCCESS -> assertNoFailureListener(response -> assertTrue(response.isAcknowledged()));
            case INVALID_NAME -> assertNoSuccessListener(
                e -> assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(InvalidIndexNameException.class))
            );
            case INVALID_SETTINGS -> assertNoSuccessListener(
                e -> assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(IllegalArgumentException.class))
            );
            case ALREADY_EXISTS -> assertNoSuccessListener(
                e -> assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(ResourceAlreadyExistsException.class))
            );
        };
    }

    private String addRandomIndexNameNoCollision(Set<String> existingIndexNames) {
        var indexName = randomIndexName();
        while (existingIndexNames.add(indexName) == false) {
            indexName = randomIndexName();
        }
        return indexName;
    }
}
