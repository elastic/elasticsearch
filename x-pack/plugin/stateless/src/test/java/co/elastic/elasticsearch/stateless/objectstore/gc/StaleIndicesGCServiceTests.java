/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.objectstore.gc;

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
