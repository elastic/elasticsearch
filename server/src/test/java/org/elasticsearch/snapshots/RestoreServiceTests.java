/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RestoreServiceTests extends ESTestCase {

    public void testUpdateDataStream() {
        long now = System.currentTimeMillis();
        String dataStreamName = "data-stream-1";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        List<Index> indices = List.of(new Index(backingIndexName, randomUUID()));
        String failureIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, now);
        List<Index> failureIndices = List.of(new Index(failureIndexName, randomUUID()));

        DataStream dataStream = DataStreamTestHelper.newInstance(dataStreamName, indices, failureIndices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);

        IndexMetadata backingIndexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(backingIndexName))).thenReturn(backingIndexMetadata);
        Index updatedBackingIndex = new Index(backingIndexName, randomUUID());
        when(backingIndexMetadata.getIndex()).thenReturn(updatedBackingIndex);

        IndexMetadata failureIndexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(failureIndexName))).thenReturn(failureIndexMetadata);
        Index updatedFailureIndex = new Index(failureIndexName, randomUUID());
        when(failureIndexMetadata.getIndex()).thenReturn(updatedFailureIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT);

        DataStream updateDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(dataStreamName, updateDataStream.getName());
        assertEquals(List.of(updatedBackingIndex), updateDataStream.getIndices());
        assertEquals(List.of(updatedFailureIndex), updateDataStream.getFailureIndices().getIndices());
    }

    public void testUpdateDataStreamRename() {
        long now = System.currentTimeMillis();
        String dataStreamName = "data-stream-1";
        String renamedDataStreamName = "data-stream-2";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String renamedBackingIndexName = DataStream.getDefaultBackingIndexName(renamedDataStreamName, 1);
        List<Index> indices = List.of(new Index(backingIndexName, randomUUID()));

        String failureIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, now);
        String renamedFailureIndexName = DataStream.getDefaultFailureStoreName(renamedDataStreamName, 1, now);
        List<Index> failureIndices = List.of(new Index(failureIndexName, randomUUID()));

        DataStream dataStream = DataStreamTestHelper.newInstance(dataStreamName, indices, failureIndices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);

        IndexMetadata backingIndexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(renamedBackingIndexName))).thenReturn(backingIndexMetadata);
        Index renamedBackingIndex = new Index(renamedBackingIndexName, randomUUID());
        when(backingIndexMetadata.getIndex()).thenReturn(renamedBackingIndex);

        IndexMetadata failureIndexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(renamedFailureIndexName))).thenReturn(failureIndexMetadata);
        Index renamedFailureIndex = new Index(renamedFailureIndexName, randomUUID());
        when(failureIndexMetadata.getIndex()).thenReturn(renamedFailureIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT).renamePattern("data-stream-1")
            .renameReplacement("data-stream-2");

        DataStream renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(List.of(renamedBackingIndex), renamedDataStream.getIndices());
        assertEquals(List.of(renamedFailureIndex), renamedDataStream.getFailureIndices().getIndices());
    }

    public void testPrefixNotChanged() {
        long now = System.currentTimeMillis();
        String dataStreamName = "ds-000001";
        String renamedDataStreamName = "ds2-000001";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String renamedBackingIndexName = DataStream.getDefaultBackingIndexName(renamedDataStreamName, 1);
        List<Index> indices = Collections.singletonList(new Index(backingIndexName, randomUUID()));

        String failureIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, now);
        String renamedFailureIndexName = DataStream.getDefaultFailureStoreName(renamedDataStreamName, 1, now);
        List<Index> failureIndices = Collections.singletonList(new Index(failureIndexName, randomUUID()));

        DataStream dataStream = DataStreamTestHelper.newInstance(dataStreamName, indices, failureIndices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(renamedBackingIndexName))).thenReturn(indexMetadata);
        Index renamedIndex = new Index(renamedBackingIndexName, randomUUID());
        when(indexMetadata.getIndex()).thenReturn(renamedIndex);

        IndexMetadata failureIndexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(renamedFailureIndexName))).thenReturn(failureIndexMetadata);
        Index renamedFailureIndex = new Index(renamedFailureIndexName, randomUUID());
        when(failureIndexMetadata.getIndex()).thenReturn(renamedFailureIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT).renamePattern("ds-").renameReplacement("ds2-");

        DataStream renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(List.of(renamedIndex), renamedDataStream.getIndices());
        assertEquals(List.of(renamedFailureIndex), renamedDataStream.getFailureIndices().getIndices());

        request = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT).renamePattern("ds-000001").renameReplacement("ds2-000001");

        renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(List.of(renamedIndex), renamedDataStream.getIndices());
        assertEquals(List.of(renamedFailureIndex), renamedDataStream.getFailureIndices().getIndices());
    }

    public void testRefreshRepositoryUuidsDoesNothingIfDisabled() {
        final RepositoriesService repositoriesService = mock(RepositoriesService.class);
        final AtomicBoolean called = new AtomicBoolean();
        RestoreService.refreshRepositoryUuids(
            false,
            repositoriesService,
            () -> assertTrue(called.compareAndSet(false, true)),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        assertTrue(called.get());
        verifyNoMoreInteractions(repositoriesService);
    }

    public void testRefreshRepositoryUuidsRefreshesAsNeeded() {
        final int repositoryCount = between(1, 5);
        final Map<String, Repository> repositories = Maps.newMapWithExpectedSize(repositoryCount);
        final Set<String> pendingRefreshes = new HashSet<>();
        final List<Runnable> finalAssertions = new ArrayList<>();
        while (repositories.size() < repositoryCount) {
            final String repositoryName = randomAlphaOfLength(10);
            switch (between(1, 3)) {
                case 1 -> {
                    final Repository notBlobStoreRepo = mock(Repository.class);
                    repositories.put(repositoryName, notBlobStoreRepo);
                    finalAssertions.add(() -> verifyNoMoreInteractions(notBlobStoreRepo));
                }
                case 2 -> {
                    final Repository freshBlobStoreRepo = mock(BlobStoreRepository.class);
                    repositories.put(repositoryName, freshBlobStoreRepo);
                    when(freshBlobStoreRepo.getMetadata()).thenReturn(
                        new RepositoryMetadata(repositoryName, randomAlphaOfLength(3), Settings.EMPTY).withUuid(UUIDs.randomBase64UUID())
                    );
                    doThrow(new AssertionError("repo UUID already known")).when(freshBlobStoreRepo).getRepositoryData(any(), any());
                }
                case 3 -> {
                    final Repository staleBlobStoreRepo = mock(BlobStoreRepository.class);
                    repositories.put(repositoryName, staleBlobStoreRepo);
                    pendingRefreshes.add(repositoryName);
                    when(staleBlobStoreRepo.getMetadata()).thenReturn(
                        new RepositoryMetadata(repositoryName, randomAlphaOfLength(3), Settings.EMPTY)
                    );
                    doAnswer(invocationOnMock -> {
                        assertTrue(pendingRefreshes.remove(repositoryName));
                        final ActionListener<RepositoryData> repositoryDataListener = invocationOnMock.getArgument(1);
                        if (randomBoolean()) {
                            repositoryDataListener.onResponse(null);
                        } else {
                            repositoryDataListener.onFailure(new Exception("simulated"));
                        }
                        return null;
                    }).when(staleBlobStoreRepo).getRepositoryData(any(), any());
                }
            }
        }

        final RepositoriesService repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.getRepositories()).thenReturn(repositories);
        final AtomicBoolean completed = new AtomicBoolean();
        RestoreService.refreshRepositoryUuids(
            true,
            repositoriesService,
            () -> assertTrue(completed.compareAndSet(false, true)),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        assertTrue(completed.get());
        assertThat(pendingRefreshes, empty());
        finalAssertions.forEach(Runnable::run);
    }

    public void testNotAllowToRestoreGlobalStateFromSnapshotWithoutOne() {

        var request = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT).includeGlobalState(true);
        var repository = new RepositoryMetadata("name", "type", Settings.EMPTY);
        var snapshot = new Snapshot("repository", new SnapshotId("name", "uuid"));

        var snapshotInfo = createSnapshotInfo(snapshot, Boolean.FALSE);

        var exception = expectThrows(
            SnapshotRestoreException.class,
            () -> RestoreService.validateSnapshotRestorable(request, repository, snapshotInfo, List.of())
        );
        assertThat(
            exception.getMessage(),
            equalTo("[name:name/uuid] cannot restore global state since the snapshot was created without global state")
        );
    }

    private static SnapshotInfo createSnapshotInfo(Snapshot snapshot, Boolean includeGlobalState) {
        var shards = randomIntBetween(0, 100);
        return new SnapshotInfo(
            snapshot,
            List.of(),
            List.of(),
            List.of(),
            randomAlphaOfLengthBetween(10, 100),
            IndexVersion.current(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            shards,
            shards,
            List.of(),
            includeGlobalState,
            Map.of(),
            SnapshotState.SUCCESS,
            Map.of()
        );
    }
}
