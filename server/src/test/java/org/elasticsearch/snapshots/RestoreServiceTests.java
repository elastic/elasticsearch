/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RestoreServiceTests extends ESTestCase {

    /**
     * Test that {@link RestoreService#warnIfIndexTemplateMissing(Map, Set, SnapshotInfo)} does not warn for system
     * datastreams.
     */
    public void testWarnIfIndexTemplateMissingSkipsSystemDataStreams() throws Exception {
        String dataStreamName = ".test-system-data-stream";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        List<Index> indices = List.of(new Index(backingIndexName, randomUUID()));

        var dataStream = DataStream.builder(dataStreamName, indices).setSystem(true).setHidden(true).build();
        var dataStreamsToRestore = Map.of(dataStreamName, dataStream);
        var templatePatterns = Set.of("matches_none");
        var snapshotInfo = createSnapshotInfo(
            new Snapshot(randomProjectIdOrDefault(), "repository", new SnapshotId("name", "uuid")),
            Boolean.FALSE
        );

        RestoreService.warnIfIndexTemplateMissing(dataStreamsToRestore, templatePatterns, snapshotInfo);

        ensureNoWarnings();
    }

    /**
     * Test that {@link RestoreService#warnIfIndexTemplateMissing(Map, Set, SnapshotInfo)} warns for non-system datastreams.
     */
    public void testWarnIfIndexTemplateMissing() throws Exception {
        String dataStreamName = ".test-system-data-stream";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        List<Index> indices = List.of(new Index(backingIndexName, randomUUID()));

        var dataStream = DataStream.builder(dataStreamName, indices).build();
        var dataStreamsToRestore = Map.of(dataStreamName, dataStream);
        var templatePatterns = Set.of("matches_none");
        var snapshotInfo = createSnapshotInfo(
            new Snapshot(randomProjectIdOrDefault(), "repository", new SnapshotId("name", "uuid")),
            Boolean.FALSE
        );

        RestoreService.warnIfIndexTemplateMissing(dataStreamsToRestore, templatePatterns, snapshotInfo);

        assertWarnings(
            format(
                "Snapshot [%s] contains data stream [%s] but custer does not have a matching index template. This will cause"
                    + " rollover to fail until a matching index template is created",
                snapshotInfo.snapshot(),
                dataStreamName
            )
        );
    }

    public void testUpdateDataStream() {
        long now = System.currentTimeMillis();
        String dataStreamName = "data-stream-1";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        List<Index> indices = List.of(new Index(backingIndexName, randomUUID()));
        String failureIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, now);
        List<Index> failureIndices = List.of(new Index(failureIndexName, randomUUID()));

        DataStream dataStream = DataStreamTestHelper.newInstance(dataStreamName, indices, failureIndices);

        ProjectMetadata.Builder metadata = mock(ProjectMetadata.Builder.class);

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
        assertEquals(List.of(updatedFailureIndex), updateDataStream.getFailureIndices());
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

        ProjectMetadata.Builder metadata = mock(ProjectMetadata.Builder.class);

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
        assertEquals(List.of(renamedFailureIndex), renamedDataStream.getFailureIndices());
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

        ProjectMetadata.Builder metadata = mock(ProjectMetadata.Builder.class);

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
        assertEquals(List.of(renamedFailureIndex), renamedDataStream.getFailureIndices());

        request = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT).renamePattern("ds-000001").renameReplacement("ds2-000001");

        renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(List.of(renamedIndex), renamedDataStream.getIndices());
        assertEquals(List.of(renamedFailureIndex), renamedDataStream.getFailureIndices());
    }

    public void testRefreshRepositoryUuidsDoesNothingIfDisabled() {
        final RepositoriesService repositoriesService = mock(RepositoriesService.class);
        final AtomicBoolean called = new AtomicBoolean();
        RestoreService.refreshRepositoryUuids(
            false,
            randomProjectIdOrDefault(),
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

        final ProjectId projectId = randomProjectIdOrDefault();
        final RepositoriesService repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.getProjectRepositories(eq(projectId))).thenReturn(repositories);
        final AtomicBoolean completed = new AtomicBoolean();
        RestoreService.refreshRepositoryUuids(
            true,
            projectId,
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
        final ProjectId projectId = randomProjectIdOrDefault();
        var snapshot = new Snapshot(projectId, "repository", new SnapshotId("name", "uuid"));

        var snapshotInfo = createSnapshotInfo(snapshot, Boolean.FALSE);

        var exception = expectThrows(
            SnapshotRestoreException.class,
            () -> RestoreService.validateSnapshotRestorable(request, repository, snapshotInfo, List.of())
        );
        assertThat(
            exception.getMessage(),
            equalTo("[" + projectId + ":name:name/uuid] cannot restore global state since the snapshot was created without global state")
        );
    }

    public void testSafeRenameIndex() {
        // Test normal rename
        String result = RestoreService.safeRenameIndex("test-index", "test", "prod");
        assertEquals("prod-index", result);

        // Test pattern that creates too-long name (255×255 case)
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestoreService.safeRenameIndex("b".repeat(255), "b", "aa")
        );
        assertThat(e.getMessage(), containsString("exceed"));

        // Test back-reference
        result = RestoreService.safeRenameIndex("test-123", "(test)-(\\d+)", "$1_$2");
        assertEquals("test_123", result);

        // Test back-reference that would be too long
        e = expectThrows(IllegalArgumentException.class, () -> RestoreService.safeRenameIndex("a".repeat(200), "(a+)", "$1$1"));
        assertThat(e.getMessage(), containsString("exceed"));

        // Test no match - returns original
        result = RestoreService.safeRenameIndex("test", "xyz", "replacement");
        assertEquals("test", result);

        // Test exactly at limit (255 chars)
        result = RestoreService.safeRenameIndex("b".repeat(255), "b+", "a".repeat(255));
        assertEquals("a".repeat(255), result);

        // Test empty replacement
        result = RestoreService.safeRenameIndex("test-index", "test-", "");
        assertEquals("index", result);

        // Test multiple matches accumulating
        result = RestoreService.safeRenameIndex("a-b-c", "-", "_");
        assertEquals("a_b_c", result);
    }

    /**
     * Tests that {@link RestoreService#prepareForReadOnlyRestore} automatically adds write block and verified_read_only
     * for read-only compatible indices (e.g. 7.x indices restored on 9.x) that were snapshotted without those flags.
     */
    public void testPrepareForReadOnlyRestore() {
        // A 7.x index without write block or verified_read_only (snapshot taken on N-2 directly)
        var indexCreated = IndexVersionUtils.randomVersionBetween(
            IndexVersions.MINIMUM_READONLY_COMPATIBLE,
            IndexVersionUtils.getPreviousVersion(IndexVersions.MINIMUM_COMPATIBLE)
        );
        var indexMetadata = IndexMetadata.builder("test-index")
            .settings(indexSettings(indexCreated, 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random())))
            .build();

        assertFalse(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings()));

        var result = RestoreService.prepareForReadOnlyRestore(
            indexMetadata,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersions.MINIMUM_READONLY_COMPATIBLE
        );

        assertTrue(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(result.getSettings()));
        assertTrue(result.getSettings().getAsBoolean("index.verified_read_only", false));
    }

    /**
     * Tests that {@link RestoreService#prepareForReadOnlyRestore} does not modify indices that are already verified
     * read-only (e.g. snapshot taken on N-1 after adding write block via the API).
     */
    public void testPrepareForReadOnlyRestoreAlreadyVerified() {
        var indexCreated = IndexVersionUtils.randomVersionBetween(
            IndexVersions.MINIMUM_READONLY_COMPATIBLE,
            IndexVersionUtils.getPreviousVersion(IndexVersions.MINIMUM_COMPATIBLE)
        );
        var indexMetadata = IndexMetadata.builder("test-index")
            .settings(
                indexSettings(indexCreated, 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
                    .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
                    .put("index.verified_read_only", true)
            )
            .build();

        var result = RestoreService.prepareForReadOnlyRestore(
            indexMetadata,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersions.MINIMUM_READONLY_COMPATIBLE
        );

        assertSame("already verified index should not be modified", indexMetadata, result);
    }

    /**
     * Tests that {@link RestoreService#prepareForReadOnlyRestore} does not modify fully compatible indices.
     */
    public void testPrepareForReadOnlyRestoreFullyCompatible() {
        var indexCreated = IndexVersionUtils.randomVersionBetween(IndexVersions.MINIMUM_COMPATIBLE, IndexVersion.current());
        var indexMetadata = IndexMetadata.builder("test-index")
            .settings(indexSettings(indexCreated, 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random())))
            .build();

        var result = RestoreService.prepareForReadOnlyRestore(
            indexMetadata,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersions.MINIMUM_READONLY_COMPATIBLE
        );

        assertSame("fully compatible index should not be modified", indexMetadata, result);
    }

    /**
     * Tests that {@link RestoreService#verifyReadOnlyRestoreSafety} skips indices that won't be auto-marked: those
     * already verified, fully supported on this version, or legacy. Such indices should not trigger any repository
     * inspection.
     */
    public void testVerifyReadOnlyRestoreSafetySkipsIndicesNotNeedingAutoMarking() {
        var snapshot = new Snapshot(randomProjectIdOrDefault(), "repo", new SnapshotId("snap", "uuid"));
        var indexId = new IndexId("idx", UUIDs.randomBase64UUID(random()));
        var repository = mock(Repository.class);

        // already verified read-only — skipped before any repository access
        var verifiedReadOnly = IndexMetadata.builder("idx")
            .settings(
                indexSettings(
                    IndexVersionUtils.randomVersionBetween(
                        IndexVersions.MINIMUM_READONLY_COMPATIBLE,
                        IndexVersionUtils.getPreviousVersion(IndexVersions.MINIMUM_COMPATIBLE)
                    ),
                    1,
                    0
                ).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
                    .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
                    .put("index.verified_read_only", true)
            )
            .build();
        RestoreService.verifyReadOnlyRestoreSafety(
            repository,
            snapshot,
            indexId,
            verifiedReadOnly,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersions.MINIMUM_READONLY_COMPATIBLE
        );

        // fully supported — skipped before any repository access
        var fullySupported = IndexMetadata.builder("idx")
            .settings(
                indexSettings(IndexVersionUtils.randomVersionBetween(IndexVersions.MINIMUM_COMPATIBLE, IndexVersion.current()), 1, 0).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    UUIDs.randomBase64UUID(random())
                )
            )
            .build();
        RestoreService.verifyReadOnlyRestoreSafety(
            repository,
            snapshot,
            indexId,
            fullySupported,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersions.MINIMUM_READONLY_COMPATIBLE
        );

        verifyNoMoreInteractions(repository);
    }

    /**
     * Tests that {@link RestoreService#verifyReadOnlyRestoreSafety} refuses to verify a snapshot served by a non-blob-store
     * repository, since commit userdata can only be inspected on blob-store repositories.
     */
    public void testVerifyReadOnlyRestoreSafetyRefusesNonBlobStoreRepository() {
        var snapshot = new Snapshot(randomProjectIdOrDefault(), "repo", new SnapshotId("snap", "uuid"));
        var indexId = new IndexId("idx", UUIDs.randomBase64UUID(random()));
        var indexCreated = IndexVersionUtils.randomVersionBetween(
            IndexVersions.MINIMUM_READONLY_COMPATIBLE,
            IndexVersionUtils.getPreviousVersion(IndexVersions.MINIMUM_COMPATIBLE)
        );
        var indexMetadata = IndexMetadata.builder("idx")
            .settings(indexSettings(indexCreated, 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random())))
            .build();

        var ex = expectThrows(
            SnapshotRestoreException.class,
            () -> RestoreService.verifyReadOnlyRestoreSafety(
                mock(Repository.class),
                snapshot,
                indexId,
                indexMetadata,
                IndexVersions.MINIMUM_COMPATIBLE,
                IndexVersions.MINIMUM_READONLY_COMPATIBLE
            )
        );
        assertThat(ex.getMessage(), containsString("snapshot commit userdata can only be inspected on blob-store repositories"));
    }

    /**
     * Tests that {@link RestoreService#verifyReadOnlyRestoreSafety} accepts a quiesced shard
     * ({@code local_checkpoint == max_seq_no}), and rejects a non-quiesced shard with a clear error.
     */
    public void testVerifyReadOnlyRestoreSafetyChecksShardQuiescence() throws Exception {
        var snapshot = new Snapshot(randomProjectIdOrDefault(), "repo", new SnapshotId("snap", "uuid"));
        var indexId = new IndexId("idx", UUIDs.randomBase64UUID(random()));
        var indexCreated = IndexVersionUtils.randomVersionBetween(
            IndexVersions.MINIMUM_READONLY_COMPATIBLE,
            IndexVersionUtils.getPreviousVersion(IndexVersions.MINIMUM_COMPATIBLE)
        );
        int numberOfShards = randomIntBetween(1, 4);
        var indexMetadata = IndexMetadata.builder("idx")
            .settings(
                indexSettings(indexCreated, numberOfShards, 0).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
            )
            .build();

        // every shard quiesced — should pass silently
        var quiescedShardSnapshot = buildShardSnapshot(snapshot.getSnapshotId().getUUID(), 42L, 42L);
        var quiescedContainer = mock(BlobContainer.class);
        var quiescedRepo = mock(BlobStoreRepository.class);
        when(quiescedRepo.shardContainer(eq(indexId), anyInt())).thenReturn(quiescedContainer);
        when(quiescedRepo.loadShardSnapshot(eq(quiescedContainer), eq(snapshot.getSnapshotId()))).thenReturn(quiescedShardSnapshot);
        RestoreService.verifyReadOnlyRestoreSafety(
            quiescedRepo,
            snapshot,
            indexId,
            indexMetadata,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersions.MINIMUM_READONLY_COMPATIBLE
        );

        // one shard with lcp < max_seq_no — should throw
        var badShard = randomIntBetween(0, numberOfShards - 1);
        var tornShardSnapshot = buildShardSnapshot(snapshot.getSnapshotId().getUUID(), 49L, 50L);
        var safeShardSnapshot = buildShardSnapshot(snapshot.getSnapshotId().getUUID(), 42L, 42L);
        var containers = new ArrayList<BlobContainer>(numberOfShards);
        for (int i = 0; i < numberOfShards; i++) {
            containers.add(mock(BlobContainer.class));
        }
        var tornRepo = mock(BlobStoreRepository.class);
        when(tornRepo.shardContainer(eq(indexId), anyInt())).thenAnswer(invocation -> containers.get(invocation.<Integer>getArgument(1)));
        for (int i = 0; i < numberOfShards; i++) {
            final var shardSnap = (i == badShard) ? tornShardSnapshot : safeShardSnapshot;
            when(tornRepo.loadShardSnapshot(eq(containers.get(i)), eq(snapshot.getSnapshotId()))).thenReturn(shardSnap);
        }
        var ex = expectThrows(
            SnapshotRestoreException.class,
            () -> RestoreService.verifyReadOnlyRestoreSafety(
                tornRepo,
                snapshot,
                indexId,
                indexMetadata,
                IndexVersions.MINIMUM_COMPATIBLE,
                IndexVersions.MINIMUM_READONLY_COMPATIBLE
            )
        );
        assertThat(ex.getMessage(), containsString("was not quiesced"));
        assertThat(ex.getMessage(), containsString("shard [" + badShard + "]"));
        assertThat(ex.getMessage(), containsString("local_checkpoint=[49]"));
        assertThat(ex.getMessage(), containsString("max_seq_no=[50]"));
    }

    /**
     * Builds a minimal {@link BlobStoreIndexShardSnapshot} whose only file is a {@code segments_N} written to an
     * in-memory Lucene directory with the given sequence-number commit data. The file bytes are inlined in the
     * {@link StoreFileMetadata} hash so that {@link RestoreService#readShardSnapshotCommitInfo} can read them without
     * accessing a real repository.
     */
    private static BlobStoreIndexShardSnapshot buildShardSnapshot(String snapshotUUID, long localCheckpoint, long maxSeqNo)
        throws IOException {
        var dir = new ByteBuffersDirectory();
        try (var writer = new IndexWriter(dir, new IndexWriterConfig())) {
            writer.setLiveCommitData(
                Map.of(
                    SequenceNumbers.LOCAL_CHECKPOINT_KEY,
                    Long.toString(localCheckpoint),
                    SequenceNumbers.MAX_SEQ_NO,
                    Long.toString(maxSeqNo)
                ).entrySet()
            );
            writer.commit();
        }
        var si = SegmentInfos.readLatestCommit(dir);
        var segmentsFileName = si.getSegmentsFileName();
        int fileLength = (int) dir.fileLength(segmentsFileName);
        var bytes = new byte[fileLength];
        try (IndexInput input = dir.openInput(segmentsFileName, IOContext.READONCE)) {
            input.readBytes(bytes, 0, fileLength);
        }
        var hash = new BytesRef(bytes);
        var checksum = Store.digestToString(CodecUtil.retrieveChecksum(new ByteArrayIndexInput(segmentsFileName, bytes, 0, fileLength)));
        var metadata = new StoreFileMetadata(
            segmentsFileName,
            fileLength,
            checksum,
            Version.LATEST.toString(),
            hash,
            StoreFileMetadata.UNAVAILABLE_WRITER_UUID
        );
        return new BlobStoreIndexShardSnapshot(
            snapshotUUID,
            List.of(new BlobStoreIndexShardSnapshot.FileInfo(segmentsFileName, metadata, null)),
            0L,
            0L,
            0,
            0L
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
