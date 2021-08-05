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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createTimestampField;
import static org.elasticsearch.mock.orig.Mockito.doThrow;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class RestoreServiceTests extends ESTestCase {

    public void testUpdateDataStream() {
        String dataStreamName = "data-stream-1";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        List<Index> indices = Collections.singletonList(new Index(backingIndexName, "uuid"));

        DataStream dataStream = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(backingIndexName))).thenReturn(indexMetadata);
        Index updatedIndex = new Index(backingIndexName, "uuid2");
        when(indexMetadata.getIndex()).thenReturn(updatedIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest();

        DataStream updateDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(dataStreamName, updateDataStream.getName());
        assertEquals(Collections.singletonList(updatedIndex), updateDataStream.getIndices());
    }

    public void testUpdateDataStreamRename() {
        String dataStreamName = "data-stream-1";
        String renamedDataStreamName = "data-stream-2";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String renamedBackingIndexName = DataStream.getDefaultBackingIndexName(renamedDataStreamName, 1);
        List<Index> indices = Collections.singletonList(new Index(backingIndexName, "uuid"));

        DataStream dataStream = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(renamedBackingIndexName))).thenReturn(indexMetadata);
        Index renamedIndex = new Index(renamedBackingIndexName, "uuid2");
        when(indexMetadata.getIndex()).thenReturn(renamedIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest().renamePattern("data-stream-1").renameReplacement("data-stream-2");

        DataStream renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(Collections.singletonList(renamedIndex), renamedDataStream.getIndices());
    }

    public void testPrefixNotChanged() {
        String dataStreamName = "ds-000001";
        String renamedDataStreamName = "ds2-000001";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String renamedBackingIndexName = DataStream.getDefaultBackingIndexName(renamedDataStreamName, 1);
        List<Index> indices = Collections.singletonList(new Index(backingIndexName, "uuid"));

        DataStream dataStream = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(renamedBackingIndexName))).thenReturn(indexMetadata);
        Index renamedIndex = new Index(renamedBackingIndexName, "uuid2");
        when(indexMetadata.getIndex()).thenReturn(renamedIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest().renamePattern("ds-").renameReplacement("ds2-");

        DataStream renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(Collections.singletonList(renamedIndex), renamedDataStream.getIndices());

        request = new RestoreSnapshotRequest().renamePattern("ds-000001").renameReplacement("ds2-000001");

        renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(Collections.singletonList(renamedIndex), renamedDataStream.getIndices());
    }

    public void testRefreshRepositoryUuidsDoesNothingIfDisabled() {
        final PlainActionFuture<Void> listener = new PlainActionFuture<>();
        final RepositoriesService repositoriesService = mock(RepositoriesService.class);
        RestoreService.refreshRepositoryUuids(false, repositoriesService, listener);
        assertTrue(listener.isDone());
        verifyZeroInteractions(repositoriesService);
    }

    public void testRefreshRepositoryUuidsRefreshesAsNeeded() throws Exception {
        final PlainActionFuture<Void> listener = new PlainActionFuture<>();

        final int repositoryCount = between(1, 5);
        final Map<String, Repository> repositories = new HashMap<>(repositoryCount);
        final Set<String> pendingRefreshes = new HashSet<>();
        final List<Runnable> finalAssertions = new ArrayList<>();
        while (repositories.size() < repositoryCount) {
            final String repositoryName = randomAlphaOfLength(10);
            switch (between(1, 3)) {
                case 1:
                    final Repository notBlobStoreRepo = mock(Repository.class);
                    repositories.put(repositoryName, notBlobStoreRepo);
                    finalAssertions.add(() -> verifyZeroInteractions(notBlobStoreRepo));
                    break;
                case 2:
                    final Repository freshBlobStoreRepo = mock(BlobStoreRepository.class);
                    repositories.put(repositoryName, freshBlobStoreRepo);
                    when(freshBlobStoreRepo.getMetadata()).thenReturn(
                        new RepositoryMetadata(repositoryName, randomAlphaOfLength(3), Settings.EMPTY).withUuid(UUIDs.randomBase64UUID())
                    );
                    doThrow(new AssertionError("repo UUID already known")).when(freshBlobStoreRepo).getRepositoryData(any());
                    break;
                case 3:
                    final Repository staleBlobStoreRepo = mock(BlobStoreRepository.class);
                    repositories.put(repositoryName, staleBlobStoreRepo);
                    pendingRefreshes.add(repositoryName);
                    when(staleBlobStoreRepo.getMetadata()).thenReturn(
                        new RepositoryMetadata(repositoryName, randomAlphaOfLength(3), Settings.EMPTY)
                    );
                    doAnswer(invocationOnMock -> {
                        assertTrue(pendingRefreshes.remove(repositoryName));
                        // noinspection unchecked
                        ActionListener<RepositoryData> repositoryDataListener = (ActionListener<RepositoryData>) invocationOnMock
                            .getArguments()[0];
                        if (randomBoolean()) {
                            repositoryDataListener.onResponse(null);
                        } else {
                            repositoryDataListener.onFailure(new Exception("simulated"));
                        }
                        return null;
                    }).when(staleBlobStoreRepo).getRepositoryData(any());
                    break;
            }
        }

        final RepositoriesService repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.getRepositories()).thenReturn(repositories);
        RestoreService.refreshRepositoryUuids(true, repositoriesService, listener);
        assertNull(listener.get(0L, TimeUnit.SECONDS));
        assertThat(pendingRefreshes, Matchers.empty());
        finalAssertions.forEach(Runnable::run);
    }

}
