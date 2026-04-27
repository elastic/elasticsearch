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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class DLMFrozenCleanupServiceTests extends ESTestCase {

    private static final String REPO_NAME = "test-repo";

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private ProjectId projectId;

    private AtomicReference<GetSnapshotsRequest> capturedGetSnapshotsRequest;
    private AtomicReference<GetSnapshotsResponse> mockGetSnapshotsResponse;
    private AtomicReference<Exception> mockGetSnapshotsFailure;

    private AtomicReference<DeleteSnapshotRequest> capturedDeleteSnapshotRequest;
    private AtomicReference<AcknowledgedResponse> mockDeleteSnapshotResponse;
    private AtomicReference<Exception> mockDeleteSnapshotFailure;

    private CopyOnWriteArrayList<DeleteIndexRequest> capturedDeleteIndexRequests;
    private AtomicReference<AcknowledgedResponse> mockDeleteIndexResponse;
    private AtomicReference<Exception> mockDeleteIndexFailure;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = createClusterService(threadPool);
        projectId = randomProjectIdOrDefault();

        capturedGetSnapshotsRequest = new AtomicReference<>();
        mockGetSnapshotsResponse = new AtomicReference<>();
        mockGetSnapshotsFailure = new AtomicReference<>();

        capturedDeleteSnapshotRequest = new AtomicReference<>();
        mockDeleteSnapshotResponse = new AtomicReference<>();
        mockDeleteSnapshotFailure = new AtomicReference<>();

        capturedDeleteIndexRequests = new CopyOnWriteArrayList<>();
        mockDeleteIndexResponse = new AtomicReference<>();
        mockDeleteIndexFailure = new AtomicReference<>();
    }

    @After
    public void cleanup() {
        clusterService.close();
        terminate(threadPool);
    }

    private NoOpClient createCapturingTestClient() {
        return new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                switch (request) {
                    case GetSnapshotsRequest req -> {
                        capturedGetSnapshotsRequest.set(req);
                        if (mockGetSnapshotsFailure.get() != null) {
                            listener.onFailure(mockGetSnapshotsFailure.get());
                        } else if (mockGetSnapshotsResponse.get() != null) {
                            listener.onResponse((Response) mockGetSnapshotsResponse.get());
                        }
                    }
                    case DeleteSnapshotRequest req -> {
                        capturedDeleteSnapshotRequest.set(req);
                        if (mockDeleteSnapshotFailure.get() != null) {
                            listener.onFailure(mockDeleteSnapshotFailure.get());
                        } else if (mockDeleteSnapshotResponse.get() != null) {
                            listener.onResponse((Response) mockDeleteSnapshotResponse.get());
                        }
                    }
                    case DeleteIndexRequest req -> {
                        capturedDeleteIndexRequests.add(req);
                        if (mockDeleteIndexFailure.get() != null) {
                            listener.onFailure(mockDeleteIndexFailure.get());
                        } else if (mockDeleteIndexResponse.get() != null) {
                            listener.onResponse((Response) mockDeleteIndexResponse.get());
                        }
                    }
                    default -> fail("Unexpected request type: " + request.getClass());
                }
            }
        };
    }

    private DLMFrozenCleanupService createService() {
        return new DLMFrozenCleanupService(clusterService, createCapturingTestClient(), TimeValue.timeValueDays(1).millis());
    }

    private IndexMetadata createDlmManagedIndex(String name) {
        return IndexMetadata.builder(name)
            .settings(settings(IndexVersion.current()).put(DLMConvertToFrozen.DLM_CREATED_SETTING_KEY, true).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private IndexMetadata createDlmManagedMountedIndex(String name, String sourceIndexName) {
        return IndexMetadata.builder(name)
            .settings(
                settings(IndexVersion.current()).put(DLMConvertToFrozen.DLM_CREATED_SETTING_KEY, true)
                    .put(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_INDEX_NAME_SETTING_KEY, sourceIndexName)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private IndexMetadata createDlmManagedClonedIndex(String name, String sourceIndexName, String sourceIndexUuid) {
        return IndexMetadata.builder(name)
            .settings(
                settings(IndexVersion.current()).put(DLMConvertToFrozen.DLM_CREATED_SETTING_KEY, true)
                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndexName)
                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, sourceIndexUuid)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private IndexMetadata createPlainIndex(String name) {
        return IndexMetadata.builder(name).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0).build();
    }

    private DataStream createDlmDatastream(IndexMetadata... backingIndices) {
        List<Index> indices = Arrays.stream(backingIndices).map(IndexMetadata::getIndex).toList();
        DataStreamLifecycle lifecycle = DataStreamLifecycle.dataLifecycleBuilder().frozenAfter(TimeValue.timeValueDays(30)).build();
        return DataStream.builder("my-ds", indices).setGeneration(1).setLifecycle(lifecycle).build();
    }

    private void setClusterState(List<IndexMetadata> indices, List<DataStream> dataStreams, String defaultRepository) {
        setClusterStateForProjects(Map.of(projectId, new TestProjectData(indices, dataStreams)), defaultRepository);
    }

    private void setClusterStateForProjects(Map<ProjectId, TestProjectData> projects, String defaultRepository) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        if (defaultRepository != null) {
            metadataBuilder.persistentSettings(
                Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), defaultRepository).build()
            );
        }
        for (Map.Entry<ProjectId, TestProjectData> entry : projects.entrySet()) {
            ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(entry.getKey());
            for (IndexMetadata idx : entry.getValue().indices()) {
                projectBuilder.put(idx, false);
            }
            for (DataStream ds : entry.getValue().dataStreams()) {
                projectBuilder.put(ds);
            }
            metadataBuilder.put(projectBuilder);
        }
        setState(clusterService, ClusterState.builder(ClusterName.DEFAULT).metadata(metadataBuilder).build());
    }

    private SnapshotInfo createSnapshotInfo(String snapshotName, List<String> snapshotIndices) {
        return createSnapshotInfo(snapshotName, snapshotIndices, Map.of(DLMConvertToFrozen.DLM_CREATED_METADATA_KEY, true));
    }

    private SnapshotInfo createSnapshotInfo(String snapshotName, List<String> snapshotIndices, Map<String, Object> userMetadata) {
        return new SnapshotInfo(
            new Snapshot(projectId, REPO_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            snapshotIndices,
            List.of(),
            List.of(),
            null,
            IndexVersion.current(),
            0L,
            0L,
            1,
            1,
            List.of(),
            false,
            userMetadata,
            SnapshotState.SUCCESS,
            Map.of()
        );
    }

    public void testNotOrphaned_cloneSourceInHealthyDataStream_nothingDeleted() {
        String sourceIndex = randomAlphaOfLength(10);
        IndexMetadata sourceMeta = createPlainIndex(sourceIndex);
        IndexMetadata cloneMeta = createDlmManagedClonedIndex(
            DLMConvertToFrozen.CLONE_INDEX_PREFIX + sourceIndex,
            sourceIndex,
            sourceMeta.getIndexUUID()
        );

        setClusterState(List.of(sourceMeta, cloneMeta), List.of(createDlmDatastream(sourceMeta)), REPO_NAME);
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(), null, 0, 0));

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteIndexRequests, empty());
        assertThat(capturedDeleteSnapshotRequest.get(), nullValue());
    }

    public void testNotOrphaned_mountedSnapshotResolvesToHealthySource_nothingDeleted() {
        String sourceIndex = randomAlphaOfLength(10);
        IndexMetadata sourceMeta = createPlainIndex(sourceIndex);
        IndexMetadata mountedMeta = createDlmManagedMountedIndex(DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + sourceIndex, sourceIndex);

        setClusterState(List.of(sourceMeta, mountedMeta), List.of(createDlmDatastream(sourceMeta)), REPO_NAME);
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(), null, 0, 0));

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteIndexRequests, empty());
    }

    public void testOrphaned_sourceMissing_cloneDeleted() {
        String sourceIndex = randomAlphaOfLength(10);
        String cloneIndex = DLMConvertToFrozen.CLONE_INDEX_PREFIX + sourceIndex;
        IndexMetadata cloneMeta = createDlmManagedClonedIndex(cloneIndex, sourceIndex, randomAlphaOfLength(10));

        setClusterState(List.of(cloneMeta), List.of(), null);
        mockDeleteIndexResponse.set(AcknowledgedResponse.TRUE);

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        DeleteIndexRequest req = capturedDeleteIndexRequests.getFirst();
        assertThat(req, not(nullValue()));
        assertThat(List.of(req.indices()), containsInAnyOrder(cloneIndex));
    }

    public void testOrphaned_sourceNotInAnyDataStream_cloneDeleted() {
        String sourceIndex = randomAlphaOfLength(10);
        String cloneIndex = DLMConvertToFrozen.CLONE_INDEX_PREFIX + sourceIndex;
        IndexMetadata sourceMeta = createPlainIndex(sourceIndex);
        IndexMetadata cloneMeta = createDlmManagedClonedIndex(cloneIndex, sourceIndex, sourceMeta.getIndexUUID());

        setClusterState(List.of(sourceMeta, cloneMeta), List.of(), null);
        mockDeleteIndexResponse.set(AcknowledgedResponse.TRUE);

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteIndexRequests, not(empty()));
        assertThat(List.of(capturedDeleteIndexRequests.getFirst().indices()), containsInAnyOrder(cloneIndex));
    }

    public void testOrphaned_mountedSnapshotOfClone_bothResolutionStepsTraverseToOrphanedSource_deleted() {
        String sourceIndex = randomAlphaOfLength(10);
        String cloneIndex = DLMConvertToFrozen.CLONE_INDEX_PREFIX + sourceIndex;
        String frozenIndex = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + sourceIndex;
        IndexMetadata sourceMeta = createPlainIndex(sourceIndex);
        IndexMetadata cloneMeta = createDlmManagedClonedIndex(cloneIndex, sourceIndex, sourceMeta.getIndexUUID());
        IndexMetadata frozenMeta = createDlmManagedMountedIndex(frozenIndex, cloneIndex);

        setClusterState(List.of(sourceMeta, cloneMeta, frozenMeta), List.of(), null);
        mockDeleteIndexResponse.set(AcknowledgedResponse.TRUE);

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        DeleteIndexRequest req = capturedDeleteIndexRequests.getFirst();
        assertThat(req, not(nullValue()));
        assertThat(List.of(req.indices()), containsInAnyOrder(cloneIndex, frozenIndex));
    }

    public void testNotOrphaned_mountedFrozenIndexIsLiveBackingIndex_nothingDeleted() {
        String sourceIndex = "foo";
        String cloneIndex = DLMConvertToFrozen.CLONE_INDEX_PREFIX + sourceIndex;
        String frozenIndex = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + sourceIndex;

        IndexMetadata frozenMeta = IndexMetadata.builder(frozenIndex)
            .settings(
                settings(IndexVersion.current()).put(DLMConvertToFrozen.DLM_CREATED_SETTING_KEY, true)
                    .put(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_INDEX_NAME_SETTING_KEY, cloneIndex)
                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndex)
                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, randomAlphaOfLength(10))
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        setClusterState(List.of(frozenMeta), List.of(createDlmDatastream(frozenMeta)), REPO_NAME);
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(), null, 0, 0));

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteIndexRequests, empty());
        assertThat(capturedDeleteSnapshotRequest.get(), nullValue());
    }

    public void testNonDlmManagedIndex_notConsidered() {
        setClusterState(List.of(createPlainIndex(randomAlphaOfLength(10))), List.of(), null);

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteIndexRequests, empty());
    }

    public void testOrphanedSnapshot_soleIndexIsOrphaned_snapshotDeleted() {
        String orphanedIndex = randomAlphaOfLength(10);
        String snapshotName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + orphanedIndex;
        SnapshotInfo orphanedSnapshot = createSnapshotInfo(snapshotName, List.of(orphanedIndex));

        setClusterState(List.of(), List.of(), REPO_NAME);
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(orphanedSnapshot), null, 1, 0));
        mockDeleteSnapshotResponse.set(AcknowledgedResponse.TRUE);

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        DeleteSnapshotRequest req = capturedDeleteSnapshotRequest.get();
        assertThat(req, not(nullValue()));
        assertThat(req.repository(), is(REPO_NAME));
        assertThat(List.of(req.snapshots()), containsInAnyOrder(snapshotName));
    }

    public void testSnapshot_multipleIndices_notDeleted() {
        String snapshotName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + randomAlphaOfLength(10);
        SnapshotInfo multiIndexSnapshot = createSnapshotInfo(snapshotName, List.of(randomAlphaOfLength(5), randomAlphaOfLength(5)));

        setClusterState(List.of(), List.of(), REPO_NAME);
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(multiIndexSnapshot), null, 1, 0));

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteSnapshotRequest.get(), nullValue());
    }

    public void testSnapshot_indexNotOrphaned_notDeleted() {
        String sourceIndex = randomAlphaOfLength(10);
        IndexMetadata sourceMeta = createPlainIndex(sourceIndex);
        SnapshotInfo snapshot = createSnapshotInfo(DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + sourceIndex, List.of(sourceIndex));

        setClusterState(List.of(sourceMeta), List.of(createDlmDatastream(sourceMeta)), REPO_NAME);
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(snapshot), null, 1, 0));

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteSnapshotRequest.get(), nullValue());
    }

    public void testSnapshot_missingDlmMetadata_notDeleted() {
        String orphanedIndex = randomAlphaOfLength(10);
        String snapshotName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + orphanedIndex;
        SnapshotInfo snapshotWithoutMetadata = createSnapshotInfo(snapshotName, List.of(orphanedIndex), null);

        setClusterState(List.of(), List.of(), REPO_NAME);
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(snapshotWithoutMetadata), null, 1, 0));

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteSnapshotRequest.get(), nullValue());
    }

    public void testSnapshot_dlmMetadataKeyAbsent_notDeleted() {
        String orphanedIndex = randomAlphaOfLength(10);
        String snapshotName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + orphanedIndex;
        SnapshotInfo snapshot = createSnapshotInfo(snapshotName, List.of(orphanedIndex), Map.of("some-other-key", true));

        setClusterState(List.of(), List.of(), REPO_NAME);
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(snapshot), null, 1, 0));

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteSnapshotRequest.get(), nullValue());
    }

    public void testNoDefaultRepository_snapshotCleanupSkipped_indicesStillCleaned() {
        String cloneIndex = DLMConvertToFrozen.CLONE_INDEX_PREFIX + randomAlphaOfLength(10);

        setClusterState(List.of(createDlmManagedIndex(cloneIndex)), List.of(), null);
        mockDeleteIndexResponse.set(AcknowledgedResponse.TRUE);

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteIndexRequests, not(empty()));
        assertThat(capturedGetSnapshotsRequest.get(), nullValue());
    }

    public void testIndexDeletion_failure_doesNotPropagate() {
        setClusterState(List.of(createDlmManagedIndex(DLMConvertToFrozen.CLONE_INDEX_PREFIX + randomAlphaOfLength(10))), List.of(), null);
        mockDeleteIndexFailure.set(new RuntimeException("deletion failed"));

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteIndexRequests, not(empty()));
    }

    public void testSnapshotDeletion_failure_doesNotPropagate() {
        String orphanedIndex = randomAlphaOfLength(10);
        String snapshotName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + orphanedIndex;

        setClusterState(List.of(), List.of(), REPO_NAME);
        mockGetSnapshotsResponse.set(
            new GetSnapshotsResponse(List.of(createSnapshotInfo(snapshotName, List.of(orphanedIndex))), null, 1, 0)
        );
        mockDeleteSnapshotFailure.set(new RuntimeException("snapshot delete failed"));

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteSnapshotRequest.get(), not(nullValue()));
    }

    public void testSnapshotListing_failure_doesNotPropagate() {
        setClusterState(List.of(), List.of(), REPO_NAME);
        mockGetSnapshotsFailure.set(new RuntimeException("listing failed"));

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedGetSnapshotsRequest.get(), not(nullValue()));
        assertThat(capturedDeleteSnapshotRequest.get(), nullValue());
    }

    public void testMultipleProjects_eachProjectCleaned() {
        ProjectId projectId2 = randomValueOtherThan(projectId, ESTestCase::randomProjectIdOrDefault);

        String clone1 = DLMConvertToFrozen.CLONE_INDEX_PREFIX + randomAlphaOfLength(10);
        String clone2 = DLMConvertToFrozen.CLONE_INDEX_PREFIX + randomAlphaOfLength(10);

        setClusterStateForProjects(
            Map.of(
                projectId,
                new TestProjectData(List.of(createDlmManagedIndex(clone1)), List.of()),
                projectId2,
                new TestProjectData(List.of(createDlmManagedIndex(clone2)), List.of())
            ),
            null
        );
        mockDeleteIndexResponse.set(AcknowledgedResponse.TRUE);

        try (DLMFrozenCleanupService service = createService()) {
            service.checkForOrphanedResources();
        }

        assertThat(capturedDeleteIndexRequests.size(), is(2));
        List<String> allDeletedIndices = capturedDeleteIndexRequests.stream().flatMap(req -> Arrays.stream(req.indices())).toList();
        assertThat(allDeletedIndices, containsInAnyOrder(clone1, clone2));
    }

    private record TestProjectData(List<IndexMetadata> indices, List<DataStream> dataStreams) {}
}
