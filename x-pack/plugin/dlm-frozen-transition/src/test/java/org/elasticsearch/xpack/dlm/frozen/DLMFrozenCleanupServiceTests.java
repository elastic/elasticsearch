/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.elasticsearch.xpack.dlm.frozen.DLMConvertToFrozen.CLONE_INDEX_PREFIX;
import static org.elasticsearch.xpack.dlm.frozen.DLMConvertToFrozen.DLM_MANAGED_METADATA_KEY;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DLMFrozenCleanupServiceTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private RecordingClient client;

    @Before
    public void setupTest() {
        Set<Setting<?>> settingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(DLMFrozenCleanupService.POLL_INTERVAL_SETTING);
        settingSet.add(RepositoriesService.DEFAULT_REPOSITORY_SETTING);
        threadPool = new TestThreadPool(getTestName());
        clusterService = createClusterService(
            threadPool,
            DiscoveryNodeUtils.create("node", "node"),
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, settingSet)
        );
        client = new RecordingClient(threadPool);
    }

    @After
    public void tearDown() throws Exception {
        clusterService.close();
        terminate(threadPool);
        super.tearDown();
    }

    public void testClusterChangedIgnoredWhenStateNotRecovered() {
        try (var service = new DLMFrozenCleanupService(clusterService, null)) {
            ClusterState stateWithBlock = ClusterState.builder(clusterService.state())
                .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
                .build();
            ClusterState previousState = ClusterState.builder(new ClusterName("test")).build();

            ClusterChangedEvent event = new ClusterChangedEvent("test", stateWithBlock, previousState);
            service.clusterChanged(event);

            assertFalse(service.isSchedulerThreadRunning());
        }
    }

    public void testBecomingMasterStartsThenLosingMasterStopsThreadPool() {
        try (var service = new DLMFrozenCleanupService(clusterService, null)) {
            assertFalse(service.isSchedulerThreadRunning());

            service.clusterChanged(createMasterEvent(true));
            assertTrue(service.isSchedulerThreadRunning());

            service.clusterChanged(createMasterEvent(false));
            assertFalse(service.isSchedulerThreadRunning());

            // Becoming master again should re-create the pool
            service.clusterChanged(createMasterEvent(true));
            assertTrue(service.isSchedulerThreadRunning());
        }
    }

    public void testRepeatedMasterEventsAreIdempotent() {
        try (var service = new DLMFrozenCleanupService(clusterService, null)) {
            service.clusterChanged(createMasterEvent(true));
            var firstRunning = service.isSchedulerThreadRunning();

            // Repeated master events should not recreate the pool
            service.clusterChanged(createMasterEvent(true));
            assertTrue(service.isSchedulerThreadRunning());
            assertEquals(firstRunning, service.isSchedulerThreadRunning());
        }
    }

    public void testRepeatedNonMasterEventsAreIdempotent() {
        try (var service = new DLMFrozenCleanupService(clusterService, null)) {
            service.clusterChanged(createMasterEvent(false));
            assertFalse(service.isSchedulerThreadRunning());

            service.clusterChanged(createMasterEvent(false));
            assertFalse(service.isSchedulerThreadRunning());
        }
    }

    public void testCloseWhileMaster() {
        var service = new DLMFrozenCleanupService(clusterService, null);
        service.clusterChanged(createMasterEvent(true));
        assertTrue(service.isSchedulerThreadRunning());

        service.close();
        assertTrue(service.isClosing());
        assertFalse(service.isSchedulerThreadRunning());

        // After close, becoming master should not start thread pool
        service.clusterChanged(createMasterEvent(true));
        assertFalse(service.isSchedulerThreadRunning());
    }

    public void testCloseWhenNeverMaster() {
        var service = new DLMFrozenCleanupService(clusterService, null);
        assertFalse(service.isSchedulerThreadRunning());

        service.close();
        assertTrue(service.isClosing());
        assertFalse(service.isSchedulerThreadRunning());
    }

    public void testCheckForOrphanedResourcesDeletesOrphanClones() {
        ProjectId projectId1 = randomUniqueProjectId();
        String sourceName1 = randomAlphaOfLength(10);
        String sourceUuid1 = randomAlphaOfLength(10);
        String cloneName1 = CLONE_INDEX_PREFIX + sourceName1;

        ProjectId projectId2 = randomUniqueProjectId();
        String sourceName2 = randomAlphaOfLength(10);
        String sourceUuid2 = randomAlphaOfLength(10);
        String cloneName2 = CLONE_INDEX_PREFIX + sourceName2;

        setClusterState(
            Settings.EMPTY,
            projectWithIndices(projectId1, createCloneIndexMetadata(cloneName1, randomAlphaOfLength(10), sourceName1, sourceUuid1)),
            projectWithIndices(projectId2, createCloneIndexMetadata(cloneName2, randomAlphaOfLength(10), sourceName2, sourceUuid2))
        );

        runCleanup();

        assertThat(client.capturedDeleteIndexRequests, hasSize(2));
        assertThat(client.capturedDeleteIndexProjectIds, containsInAnyOrder(projectId1, projectId2));
        for (int i = 0; i < client.capturedDeleteIndexRequests.size(); i++) {
            DeleteIndexRequest request = client.capturedDeleteIndexRequests.get(i);
            ProjectId projectId = client.capturedDeleteIndexProjectIds.get(i);
            assertThat(request.indicesOptions(), equalTo(DLMConvertToFrozen.IGNORE_MISSING_OPTIONS));
            if (projectId.equals(projectId1)) {
                assertThat(request.indices(), arrayContaining(cloneName1));
            } else {
                assertThat(request.indices(), arrayContaining(cloneName2));
            }
        }
        assertThat(client.capturedDeleteSnapshotRequests, empty());
    }

    public void testCheckForOrphanedResourcesSkipsCloneWithoutResizeSource() {
        ProjectId projectId = randomUniqueProjectId();
        String cloneName = CLONE_INDEX_PREFIX + randomAlphaOfLength(10);
        setClusterState(Settings.EMPTY, projectWithIndices(projectId, createIndexMetadata(cloneName, randomAlphaOfLength(10))));

        runCleanup();

        assertThat(client.capturedDeleteIndexRequests, empty());
        assertThat(client.capturedGetSnapshotsRequest, nullValue());
    }

    public void testCheckForOrphanedResourcesDeletesCloneWhenSourceExistsInOtherProjectOnly() {
        ProjectId orphanedCloneProjectId = randomUniqueProjectId();
        ProjectId sourceProjectId = randomUniqueProjectId();
        String sourceName = randomAlphaOfLength(10);
        String sourceUuid = randomAlphaOfLength(10);
        String cloneName = CLONE_INDEX_PREFIX + sourceName;
        setClusterState(
            Settings.EMPTY,
            projectWithIndices(
                orphanedCloneProjectId,
                createCloneIndexMetadata(cloneName, randomAlphaOfLength(10), sourceName, sourceUuid)
            ),
            projectWithIndices(sourceProjectId, createIndexMetadata(sourceName, sourceUuid))
        );

        runCleanup();

        assertThat(client.capturedDeleteIndexRequests, hasSize(1));
        assertThat(client.capturedDeleteIndexProjectIds.getFirst(), equalTo(orphanedCloneProjectId));
        assertThat(client.capturedDeleteIndexRequests.getFirst().indices(), arrayContaining(cloneName));
    }

    public void testCheckForOrphanedResourcesSkipsCloneWhenSourceExistsInSameProject() {
        ProjectId projectId = randomUniqueProjectId();
        String sourceName = randomAlphaOfLength(10);
        String sourceUuid = randomAlphaOfLength(10);
        String cloneName = CLONE_INDEX_PREFIX + sourceName;
        setClusterState(
            Settings.EMPTY,
            projectWithIndices(
                projectId,
                createCloneIndexMetadata(cloneName, randomAlphaOfLength(10), sourceName, sourceUuid),
                createIndexMetadata(sourceName, sourceUuid)
            )
        );

        runCleanup();

        assertThat(client.capturedDeleteIndexRequests, empty());
        assertThat(client.capturedGetSnapshotsRequest, nullValue());
    }

    public void testCheckForOrphanedResourcesSkipsSnapshotWhenIndexExistsInSameProject() {
        String repository = randomAlphaOfLength(8);
        String snapshotIndexName = randomAlphaOfLength(10);
        ProjectId projectId = randomUniqueProjectId();
        setClusterStateWithRepository(
            repository,
            projectWithIndices(projectId, createIndexMetadata(snapshotIndexName, randomAlphaOfLength(10)))
        );
        client.getSnapshotsResponse = new GetSnapshotsResponse(
            List.of(
                createSnapshotInfo(repository, randomAlphaOfLength(10), List.of(snapshotIndexName), Map.of(DLM_MANAGED_METADATA_KEY, true))
            ),
            null,
            1,
            0
        );

        runCleanup();

        assertThat(client.capturedGetSnapshotsRequest, notNullValue());
        assertThat(client.capturedDeleteSnapshotRequests, empty());
    }

    public void testCheckForOrphanedResourcesSkipsSnapshotWithNullMetadata() {
        String repository = randomAlphaOfLength(8);
        String orphanedIndexName = randomAlphaOfLength(10);
        setClusterStateWithRepository(repository, emptyProjectMetadata());
        client.getSnapshotsResponse = new GetSnapshotsResponse(
            List.of(createSnapshotInfo(repository, randomAlphaOfLength(10), List.of(orphanedIndexName), null)),
            null,
            1,
            0
        );

        runCleanup();

        assertThat(client.capturedGetSnapshotsRequest, notNullValue());
        assertThat(client.capturedDeleteSnapshotRequests, empty());
    }

    public void testCheckForOrphanedResourcesSkipsSnapshotWithEmptyMetadata() {
        String repository = randomAlphaOfLength(8);
        String orphanedIndexName = randomAlphaOfLength(10);
        setClusterStateWithRepository(repository, emptyProjectMetadata());
        client.getSnapshotsResponse = new GetSnapshotsResponse(
            List.of(createSnapshotInfo(repository, randomAlphaOfLength(10), List.of(orphanedIndexName), Map.of())),
            null,
            1,
            0
        );

        runCleanup();

        assertThat(client.capturedGetSnapshotsRequest, notNullValue());
        assertThat(client.capturedDeleteSnapshotRequests, empty());
    }

    public void testCheckForOrphanedResourcesSkipsSnapshotWithDlmManagedFalse() {
        String repository = randomAlphaOfLength(8);
        String orphanedIndexName = randomAlphaOfLength(10);
        setClusterStateWithRepository(repository, emptyProjectMetadata());
        client.getSnapshotsResponse = new GetSnapshotsResponse(
            List.of(
                createSnapshotInfo(repository, randomAlphaOfLength(10), List.of(orphanedIndexName), Map.of(DLM_MANAGED_METADATA_KEY, false))
            ),
            null,
            1,
            0
        );

        runCleanup();

        assertThat(client.capturedGetSnapshotsRequest, notNullValue());
        assertThat(client.capturedDeleteSnapshotRequests, empty());
    }

    public void testCheckForOrphanedResourcesSkipsSnapshotWithDlmManagedStringInsteadOfBoolean() {
        String repository = randomAlphaOfLength(8);
        String orphanedIndexName = randomAlphaOfLength(10);
        setClusterStateWithRepository(repository, emptyProjectMetadata());
        client.getSnapshotsResponse = new GetSnapshotsResponse(
            List.of(
                createSnapshotInfo(
                    repository,
                    randomAlphaOfLength(10),
                    List.of(orphanedIndexName),
                    Map.of(DLM_MANAGED_METADATA_KEY, "true")
                )
            ),
            null,
            1,
            0
        );

        runCleanup();

        assertThat(client.capturedGetSnapshotsRequest, notNullValue());
        assertThat(client.capturedDeleteSnapshotRequests, empty());
    }

    public void testCheckForOrphanedResourcesSkipsSnapshotWithMultipleIndices() {
        String repository = randomAlphaOfLength(8);
        setClusterStateWithRepository(repository, emptyProjectMetadata());
        client.getSnapshotsResponse = new GetSnapshotsResponse(
            List.of(
                createSnapshotInfo(
                    repository,
                    randomAlphaOfLength(10),
                    List.of(randomAlphaOfLength(10), randomAlphaOfLength(10)),
                    Map.of(DLM_MANAGED_METADATA_KEY, true)
                )
            ),
            null,
            1,
            0
        );

        runCleanup();

        assertThat(client.capturedGetSnapshotsRequest, notNullValue());
        assertThat(client.capturedDeleteSnapshotRequests, empty());
    }

    public void testCheckForOrphanedResourcesIgnoresSnapshotListingFailure() {
        String repository = randomAlphaOfLength(8);
        setClusterStateWithRepository(repository, emptyProjectMetadata());
        client.getSnapshotsFailure = new RuntimeException("snapshot listing exception");

        runCleanup();

        assertThat(client.capturedGetSnapshotsRequest, notNullValue());
        assertThat(client.capturedDeleteSnapshotRequests, empty());
    }

    public void testCheckForOrphanedSnapshotsQueriesEachProjectSeparately() {
        String repository = randomAlphaOfLength(8);
        ProjectId projectId1 = randomUniqueProjectId();
        ProjectId projectId2 = randomUniqueProjectId();
        setClusterStateWithRepository(repository, ProjectMetadata.builder(projectId1), ProjectMetadata.builder(projectId2));

        runCleanup();

        assertThat(client.capturedGetSnapshotsProjectIds, hasSize(2));
        assertThat(client.capturedGetSnapshotsProjectIds, containsInAnyOrder(projectId1, projectId2));
        assertThat(client.capturedDeleteSnapshotRequests, empty());
    }

    private void runCleanup() {
        try (var service = new DLMFrozenCleanupService(clusterService, client)) {
            service.checkForOrphanedResources();
        }
    }

    private ClusterChangedEvent createMasterEvent(boolean isMaster) {
        var localNode = DiscoveryNodeUtils.create("local-node", "local-node");
        var otherNode = DiscoveryNodeUtils.create("other-node", "other-node");

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId("local-node");

        if (isMaster) {
            nodesBuilder.masterNodeId("local-node");
        } else {
            nodesBuilder.masterNodeId("other-node");
        }

        ClusterState newState = ClusterState.builder(clusterService.state())
            .nodes(nodesBuilder)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        ClusterState previousState = ClusterState.builder(new ClusterName("test"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(localNode)
                    .add(otherNode)
                    .localNodeId("local-node")
                    .masterNodeId(isMaster ? "other-node" : "local-node")
            )
            .build();

        return new ClusterChangedEvent("test", newState, previousState);
    }

    private void setClusterState(Settings persistentSettings, ProjectMetadata.Builder... projects) {
        Metadata.Builder metadataBuilder = Metadata.builder().persistentSettings(persistentSettings);
        for (ProjectMetadata.Builder project : projects) {
            metadataBuilder.put(project);
        }
        setState(clusterService, ClusterState.builder(new ClusterName("test")).metadata(metadataBuilder.build()).build());
    }

    private void setClusterStateWithRepository(String repository, ProjectMetadata.Builder... projects) {
        setClusterState(Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), repository).build(), projects);
    }

    private ProjectMetadata.Builder emptyProjectMetadata() {
        return ProjectMetadata.builder(randomUniqueProjectId());
    }

    private ProjectMetadata.Builder projectWithIndices(ProjectId projectId, IndexMetadata... indices) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        for (IndexMetadata indexMetadata : indices) {
            builder.put(indexMetadata, false);
        }
        return builder;
    }

    private IndexMetadata createIndexMetadata(String indexName, String indexUuid) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private IndexMetadata createCloneIndexMetadata(String indexName, String indexUuid, String sourceName, String sourceUuid) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), sourceName)
                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.getKey(), sourceUuid)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private SnapshotInfo createSnapshotInfo(String repository, String snapshotName, List<String> indices, Map<String, Object> metadata) {
        return new SnapshotInfo(
            new Snapshot(repository, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            indices,
            List.of(),
            List.of(),
            null,
            IndexVersion.current(),
            0L,
            0L,
            0,
            0,
            List.of(),
            Boolean.FALSE,
            metadata,
            SnapshotState.SUCCESS,
            Map.of()
        );
    }

    private class RecordingClient extends NoOpClient {
        private final List<DeleteIndexRequest> capturedDeleteIndexRequests = new ArrayList<>();
        private final List<ProjectId> capturedDeleteIndexProjectIds = new ArrayList<>();
        private final List<ProjectId> capturedGetSnapshotsProjectIds = new ArrayList<>();
        private final Map<ProjectId, GetSnapshotsResponse> getSnapshotsResponseByProject = new HashMap<>();
        private final Map<ProjectId, Exception> getSnapshotsFailureByProject = new HashMap<>();
        private final List<DeleteSnapshotRequest> capturedDeleteSnapshotRequests = new ArrayList<>();
        private final List<ProjectId> capturedDeleteSnapshotProjectIds = new ArrayList<>();
        private final AcknowledgedResponse deleteIndexResponse = AcknowledgedResponse.of(true);
        private final AcknowledgedResponse deleteSnapshotResponse = AcknowledgedResponse.of(true);
        private Exception deleteIndexFailure;
        private GetSnapshotsRequest capturedGetSnapshotsRequest;
        private GetSnapshotsResponse getSnapshotsResponse = new GetSnapshotsResponse(List.of(), null, 0, 0);
        private Exception getSnapshotsFailure;
        private DeleteSnapshotRequest capturedDeleteSnapshotRequest;

        private RecordingClient(TestThreadPool threadPool) {
            super(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext()));
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            switch (request) {
                case DeleteIndexRequest deleteIndexRequest -> {
                    capturedDeleteIndexRequests.add(deleteIndexRequest);
                    String projectIdHeader = threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
                    capturedDeleteIndexProjectIds.add(
                        projectIdHeader == null ? Metadata.DEFAULT_PROJECT_ID : ProjectId.fromId(projectIdHeader)
                    );
                    if (deleteIndexFailure != null) {
                        listener.onFailure(deleteIndexFailure);
                    } else {
                        listener.onResponse((Response) deleteIndexResponse);
                    }
                }
                case GetSnapshotsRequest getSnapshotsRequest -> {
                    capturedGetSnapshotsRequest = getSnapshotsRequest;
                    String projectIdHeader = threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
                    ProjectId requestProjectId = projectIdHeader == null ? Metadata.DEFAULT_PROJECT_ID : ProjectId.fromId(projectIdHeader);
                    capturedGetSnapshotsProjectIds.add(requestProjectId);
                    Exception failure = getSnapshotsFailureByProject.getOrDefault(requestProjectId, getSnapshotsFailure);
                    if (failure != null) {
                        listener.onFailure(failure);
                    } else {
                        GetSnapshotsResponse response = getSnapshotsResponseByProject.getOrDefault(requestProjectId, getSnapshotsResponse);
                        listener.onResponse((Response) response);
                    }
                }
                case DeleteSnapshotRequest deleteSnapshotRequest -> {
                    capturedDeleteSnapshotRequest = deleteSnapshotRequest;
                    capturedDeleteSnapshotRequests.add(deleteSnapshotRequest);
                    String projectIdHeader = threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
                    ProjectId requestProjectId = projectIdHeader == null ? Metadata.DEFAULT_PROJECT_ID : ProjectId.fromId(projectIdHeader);
                    capturedDeleteSnapshotProjectIds.add(requestProjectId);
                    listener.onResponse((Response) deleteSnapshotResponse);
                }
                case null, default -> fail("unexpected action: " + action.name());
            }
        }
    }
}
