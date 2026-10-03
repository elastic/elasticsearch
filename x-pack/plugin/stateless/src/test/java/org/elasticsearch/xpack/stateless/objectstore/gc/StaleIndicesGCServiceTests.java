/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore.gc;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class StaleIndicesGCServiceTests extends ESTestCase {

    public void testGetStaleIndicesUUIDsSkipsRemovingProject() throws IOException {
        final ObjectStoreService objectStoreService = createObjectStoreService();
        final var removingProject = randomUniqueProjectId();
        when(objectStoreService.getIndicesBlobContainer(removingProject)).thenThrow(
            new RepositoryException("stateless", "removing project")
        );

        final var goodProject = randomUniqueProjectId();
        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(objectStoreService.getIndicesBlobContainer(goodProject)).thenReturn(blobContainer);
        when(blobContainer.children(eq(OperationPurpose.INDICES))).thenReturn(Map.of("stale-index", mock(BlobContainer.class)));

        final ClusterService clusterService = mock(ClusterService.class);
        final var stateBuilder = ClusterState.builder(org.elasticsearch.cluster.ClusterState.EMPTY_STATE);
        stateBuilder.putProjectMetadata(ProjectMetadata.builder(removingProject));
        stateBuilder.putProjectMetadata(ProjectMetadata.builder(goodProject));
        when(clusterService.state()).thenReturn(stateBuilder.build());

        StaleIndicesGCService service = new StaleIndicesGCService(
            () -> objectStoreService,
            clusterService,
            mock(ThreadPool.class),
            mock(Client.class)
        );

        final var staleIndices = service.getStaleIndicesUUIDs();
        assertThat(staleIndices, equalTo(Map.of(goodProject, Set.of("stale-index"))));
    }

    public void testDeleteStaleIndicesSkipsRemovingProject() throws IOException {
        final var removedProject = randomUniqueProjectId();
        final var removingProject = randomUniqueProjectId();
        final var goodProject = randomUniqueProjectId();

        final ObjectStoreService objectStoreService = createObjectStoreService();
        when(objectStoreService.getIndexBlobContainer(eq(removingProject), anyString())).thenThrow(
            new RepositoryException("stateless", "removing project")
        );

        final CountDownLatch deletionLatch = new CountDownLatch(1);
        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.delete(eq(OperationPurpose.INDICES))).thenAnswer(invocation -> {
            deletionLatch.countDown();
            return new DeleteResult(1, 42);
        });
        when(objectStoreService.getIndexBlobContainer(goodProject, "index_of_good_project")).thenReturn(blobContainer);

        StaleIndicesGCService service = new StaleIndicesGCService(
            () -> objectStoreService,
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(Client.class)
        );

        final var stateBuilder = ClusterState.builder(org.elasticsearch.cluster.ClusterState.EMPTY_STATE);
        stateBuilder.putProjectMetadata(ProjectMetadata.builder(removingProject));
        stateBuilder.putProjectMetadata(ProjectMetadata.builder(goodProject));

        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        service.deleteStaleIndices(
            future,
            stateBuilder.build(),
            Map.of(
                removedProject,
                Set.of("index_of_removed_project"), // project is removed already
                removingProject,
                Set.of("index_of_removing_project"), // project is being removed
                goodProject,
                Set.of("index_of_good_project") // a regular running project
            )
        );

        safeGet(future);
        safeAwait(deletionLatch);
    }

    public void testDeleteStaleIndicesSkipsExistingIndexInLatestClusterState() throws IOException {
        final var projectId = randomUniqueProjectId();
        final var indexUUIDToDelete = randomUUID();
        final var stillValidIndexUUID = randomUUID();
        final ObjectStoreService objectStoreService = createObjectStoreService();
        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(objectStoreService.getIndexBlobContainer(eq(projectId), eq(indexUUIDToDelete))).thenReturn(blobContainer);

        final var service = new StaleIndicesGCService(
            () -> objectStoreService,
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(Client.class)
        );

        final var stateBuilder = ClusterState.builder(org.elasticsearch.cluster.ClusterState.EMPTY_STATE);
        stateBuilder.putProjectMetadata(
            ProjectMetadata.builder(projectId)
                .put(
                    IndexMetadata.builder("common-index")
                        .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, stillValidIndexUUID))
                )
        );

        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        service.deleteStaleIndices(future, stateBuilder.build(), Map.of(projectId, Set.of(indexUUIDToDelete, stillValidIndexUUID)));

        safeGet(future);
        verify(objectStoreService).getIndexBlobContainer(projectId, indexUUIDToDelete);
        verify(blobContainer).delete(OperationPurpose.INDICES);
        verifyNoMoreInteractions(objectStoreService, blobContainer);
    }

    public void testRemovedShardCleanupPreservesCurrentAndNewFiles() throws IOException {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var indexPath = BlobPath.EMPTY.add("indices").add("uuid");
            final var live = store.blobContainer(indexPath.add("0").add("10"));
            final var removed = store.blobContainer(indexPath.add("1").add("5"));
            live.writeBlob(OperationPurpose.INDICES, "live", new BytesArray("live"), true);
            removed.writeBlob(OperationPurpose.INDICES, "old", new BytesArray("old"), true);
            // Mock only the service wiring; discovery and deletion operate on a real filesystem blob store.
            final var objectStore = mock(ObjectStoreService.class);
            when(objectStore.getIndexBlobContainer(ProjectId.DEFAULT, "uuid")).thenReturn(store.blobContainer(indexPath));
            final var clusterService = mock(ClusterService.class);
            final var state = restoredState(1, "history");
            when(clusterService.state()).thenReturn(state);
            final var service = new StaleIndicesGCService(() -> objectStore, clusterService, mock(ThreadPool.class), mock(Client.class));
            final var candidates = service.getStaleShardFiles();
            assertEquals(1, candidates.size());
            final var reintroduced = store.blobContainer(indexPath.add("1").add("11"));
            reintroduced.writeBlob(OperationPurpose.INDICES, "new", new BytesArray("new"), true);
            final var future = new PlainActionFuture<Void>();
            service.deleteStaleShardFiles(future, state, candidates);
            safeGet(future);
            assertFalse(removed.blobExists(OperationPurpose.INDICES, "old"));
            assertTrue(live.blobExists(OperationPurpose.INDICES, "live"));
            assertTrue(reintroduced.blobExists(OperationPurpose.INDICES, "new"));
        }
    }

    public void testRemovedShardCleanupUsesBoundedBatches() throws IOException {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var indexPath = BlobPath.EMPTY.add("indices").add("uuid");
            final var live = store.blobContainer(indexPath.add("0").add("10"));
            live.writeBlob(OperationPurpose.INDICES, "live", new BytesArray("live"), true);
            // Cross both shard and nested-container boundaries, with more than a batch in each container.
            for (int shard = 1; shard <= 2; shard++) {
                for (int term = 1; term <= 2; term++) {
                    final var container = store.blobContainer(indexPath.add(Integer.toString(shard)).add(Integer.toString(term)));
                    for (int file = 0; file < 5; file++) {
                        container.writeBlob(OperationPurpose.INDICES, "file-" + file, new BytesArray("old"), true);
                    }
                }
            }
            // Mock only the service wiring; listing and deletion use the real filesystem blob store.
            final var objectStore = mock(ObjectStoreService.class);
            when(objectStore.getIndexBlobContainer(ProjectId.DEFAULT, "uuid")).thenReturn(store.blobContainer(indexPath));
            final var clusterService = mock(ClusterService.class);
            final var state = restoredState(1, "history");
            when(clusterService.state()).thenReturn(state);
            final var service = new StaleIndicesGCService(() -> objectStore, clusterService, mock(ThreadPool.class), mock(Client.class));
            final int batchSize = 3;
            int remaining = 20;
            while (remaining > 0) {
                final var candidates = service.getStaleShardFiles(batchSize);
                final int files = candidates.stream().mapToInt(candidate -> candidate.names().size()).sum();
                assertEquals(Math.min(batchSize, remaining), files);
                // A changed history must invalidate the whole batch, without preventing a subsequent cycle from retrying it.
                final var skipped = new PlainActionFuture<Void>();
                service.deleteStaleShardFiles(skipped, restoredState(1, "new-history"), candidates);
                safeGet(skipped);
                for (var candidate : candidates) {
                    for (String name : candidate.names()) {
                        assertTrue(candidate.container().blobExists(OperationPurpose.INDICES, name));
                    }
                }
                final var deleted = new PlainActionFuture<Void>();
                service.deleteStaleShardFiles(deleted, state, candidates);
                safeGet(deleted);
                for (var candidate : candidates) {
                    for (String name : candidate.names()) {
                        assertFalse(candidate.container().blobExists(OperationPurpose.INDICES, name));
                    }
                }
                remaining -= files;
            }
            assertTrue(service.getStaleShardFiles(batchSize).isEmpty());
            assertTrue(live.blobExists(OperationPurpose.INDICES, "live"));
        }
    }

    public void testRemovedShardCleanupRechecksConsistentState() throws IOException {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var indexPath = BlobPath.EMPTY.add("indices").add("uuid");
            final var removed = store.blobContainer(indexPath.add("1").add("5"));
            removed.writeBlob(OperationPurpose.INDICES, "old", new BytesArray("old"), true);
            final var objectStore = mock(ObjectStoreService.class);
            when(objectStore.getIndexBlobContainer(ProjectId.DEFAULT, "uuid")).thenReturn(store.blobContainer(indexPath));
            final var clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(restoredState(1, "history"));
            final var service = new StaleIndicesGCService(() -> objectStore, clusterService, mock(ThreadPool.class), mock(Client.class));
            final var candidates = service.getStaleShardFiles();
            assertEquals(1, candidates.size());
            for (var state : List.of(restoredState(2, "history"), restoredState(1, "new-history"))) {
                final var future = new PlainActionFuture<Void>();
                service.deleteStaleShardFiles(future, state, candidates);
                safeGet(future);
                assertTrue(removed.blobExists(OperationPurpose.INDICES, "old"));
            }
        }
    }

    private static ClusterState restoredState(int shards, String history) {
        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(
                ProjectMetadata.builder(ProjectId.DEFAULT)
                    .put(
                        IndexMetadata.builder("index")
                            .settings(
                                indexSettings(IndexVersion.current(), shards, 0).put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                                    .put(IndexMetadata.SETTING_HISTORY_UUID, history)
                            )
                    )
            )
            .build();
    }

    private ObjectStoreService createObjectStoreService() throws IOException {
        final var objectStoreService = mock(ObjectStoreService.class);
        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(objectStoreService.getIndicesBlobContainer(ProjectId.DEFAULT)).thenReturn(blobContainer);
        when(objectStoreService.getIndexBlobContainer(eq(ProjectId.DEFAULT), anyString())).thenReturn(blobContainer);
        when(blobContainer.children(eq(OperationPurpose.INDICES))).thenReturn(Map.of());
        when(blobContainer.delete(eq(OperationPurpose.INDICES))).thenReturn(new DeleteResult(0, 0));
        return objectStoreService;
    }
}
