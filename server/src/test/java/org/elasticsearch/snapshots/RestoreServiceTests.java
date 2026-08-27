/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryFeatures;
import org.elasticsearch.repositories.IndexId;
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

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.containsString;
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

    // ---- isRestoringShard predicate tests --------------------------------------------------

    /**
     * Bundles the objects needed to exercise {@link RestoreService#isRestoringShard} with all six correlation
     * conditions satisfied. Individual tests override specific parts to verify each failing condition.
     */
    private record RestoreTestState(ShardRouting primary, RestoreInProgress restoreInProgress, Snapshot snapshot) {}

    /**
     * Builds a {@link RestoreTestState} in which every predicate condition holds: the primary is
     * {@code INITIALIZING} with a {@link SnapshotRecoverySource}, a matching {@link RestoreInProgress}
     * entry exists, the source snapshot matches, the exact {@link ShardId} is present, and the shard's
     * restore status is {@link RestoreInProgress.State#STARTED} (not completed).
     */
    private static RestoreTestState buildRestoreTestState() {
        String restoreUuid = randomUUID();
        String repoName = randomIdentifier();
        String indexName = randomIdentifier();
        String indexUuid = randomUUID();
        String nodeId = randomUUID();

        Snapshot snapshot = new Snapshot(randomProjectIdOrDefault(), repoName, new SnapshotId(randomIdentifier(), randomUUID()));
        ShardId shardId = new ShardId(indexName, indexUuid, 0);

        ShardRouting primary = TestShardRouting.shardRoutingBuilder(shardId, nodeId, true, ShardRoutingState.INITIALIZING)
            .withRecoverySource(
                new SnapshotRecoverySource(restoreUuid, snapshot, IndexVersion.current(), new IndexId(indexName, indexUuid))
            )
            .build();

        RestoreInProgress restoreInProgress = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                restoreUuid,
                snapshot,
                RestoreInProgress.State.STARTED,
                false,
                List.of(indexName),
                Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(nodeId))
            )
        ).build();

        return new RestoreTestState(primary, restoreInProgress, snapshot);
    }

    /**
     * Baseline: all six conditions met, restore status {@code STARTED} — predicate must return {@code true}.
     */
    public void testIsRestoringShard_allConditionsMet_returnsTrue() {
        var s = buildRestoreTestState();
        assertTrue(RestoreService.isRestoringShard(s.restoreInProgress(), s.primary()));
    }

    /**
     * Condition 1: recovery source is not a {@link SnapshotRecoverySource} — must return {@code false}.
     * Covers peer recovery, empty-store, and existing-store allocations.
     */
    public void testIsRestoringShard_nonSnapshotRecoverySource_returnsFalse() {
        var s = buildRestoreTestState();
        ShardRouting peerRecovery = TestShardRouting.shardRoutingBuilder(
            s.primary().shardId(),
            s.primary().currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        ).withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE).build();

        assertFalse(RestoreService.isRestoringShard(s.restoreInProgress(), peerRecovery));
    }

    /**
     * Condition 2: {@code restoreUUID} is {@link SnapshotRecoverySource#NO_API_RESTORE_UUID} — must return {@code false}.
     * This sentinel marks searchable-snapshot allocations, which must not be treated as API-level restores.
     */
    public void testIsRestoringShard_noApiRestoreUuid_returnsFalse() {
        var s = buildRestoreTestState();
        ShardId shardId = s.primary().shardId();
        ShardRouting noApiRouting = TestShardRouting.shardRoutingBuilder(
            shardId,
            s.primary().currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        )
            .withRecoverySource(
                new SnapshotRecoverySource(
                    SnapshotRecoverySource.NO_API_RESTORE_UUID,
                    s.snapshot(),
                    IndexVersion.current(),
                    new IndexId(shardId.getIndexName(), randomUUID())
                )
            )
            .build();

        assertFalse(RestoreService.isRestoringShard(s.restoreInProgress(), noApiRouting));
    }

    /**
     * Condition 3: UUID present in the routing's recovery source has no matching entry in {@link RestoreInProgress}
     * (stale routing) — must return {@code false}.
     */
    public void testIsRestoringShard_staleUuid_returnsFalse() {
        var s = buildRestoreTestState();
        // EMPTY has no entries at all, so the UUID lookup returns null
        assertFalse(RestoreService.isRestoringShard(RestoreInProgress.EMPTY, s.primary()));
    }

    /**
     * Condition 3 (variant): {@link RestoreInProgress} has an active entry, but it is keyed under a different
     * UUID than the one in the shard's routing recovery source — UUID lookup still returns {@code null}, so
     * the predicate must return {@code false}.
     */
    public void testIsRestoringShard_staleUuidWithOtherEntry_returnsFalse() {
        var s = buildRestoreTestState();
        String otherUuid = randomUUID();
        ShardId otherShardId = new ShardId(randomIdentifier(), randomUUID(), 0);
        Snapshot otherSnapshot = new Snapshot(
            randomProjectIdOrDefault(),
            randomIdentifier(),
            new SnapshotId(randomIdentifier(), randomUUID())
        );

        RestoreInProgress unrelatedRestoreInProgress = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                otherUuid,
                otherSnapshot,
                RestoreInProgress.State.STARTED,
                false,
                List.of(otherShardId.getIndexName()),
                Map.of(otherShardId, new RestoreInProgress.ShardRestoreStatus(randomUUID()))
            )
        ).build();

        assertFalse(RestoreService.isRestoringShard(unrelatedRestoreInProgress, s.primary()));
    }

    /**
     * Condition 4: an entry exists for the UUID but its {@link Snapshot} differs from the routing's recovery source
     * (mismatched correlation state) — must return {@code false}.
     */
    public void testIsRestoringShard_snapshotMismatch_throwsAssertionError() {
        var s = buildRestoreTestState();
        SnapshotRecoverySource source = (SnapshotRecoverySource) s.primary().recoverySource();
        ShardId shardId = s.primary().shardId();

        Snapshot differentSnapshot = new Snapshot(
            randomProjectIdOrDefault(),
            randomIdentifier(),
            new SnapshotId(randomIdentifier(), randomUUID())
        );
        RestoreInProgress mismatchedRestore = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                source.restoreUUID(),
                differentSnapshot,
                RestoreInProgress.State.STARTED,
                false,
                List.of(shardId.getIndexName()),
                Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(s.primary().currentNodeId()))
            )
        ).build();

        assertThrows(AssertionError.class, () -> RestoreService.isRestoringShard(mismatchedRestore, s.primary()));
    }

    /**
     * Condition 5: entry has the right UUID and snapshot but does not contain this exact {@link ShardId}
     * (entry covers a subset of shards) — must return {@code false}.
     */
    public void testIsRestoringShard_shardIdAbsent_returnsFalse() {
        var s = buildRestoreTestState();
        SnapshotRecoverySource source = (SnapshotRecoverySource) s.primary().recoverySource();
        ShardId shardId = s.primary().shardId();

        ShardId otherShardId = new ShardId(shardId.getIndexName(), shardId.getIndex().getUUID(), shardId.id() + 1);
        RestoreInProgress noShardRestore = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                source.restoreUUID(),
                s.snapshot(),
                RestoreInProgress.State.STARTED,
                false,
                List.of(shardId.getIndexName()),
                Map.of(otherShardId, new RestoreInProgress.ShardRestoreStatus(s.primary().currentNodeId()))
            )
        ).build();

        assertFalse(RestoreService.isRestoringShard(noShardRestore, s.primary()));
    }

    /**
     * Condition 6: shard restore status is {@link RestoreInProgress.State#SUCCESS} (completed) — must return {@code false}.
     */
    public void testIsRestoringShard_restoreStatusSuccess_returnsFalse() {
        var s = buildRestoreTestState();
        SnapshotRecoverySource source = (SnapshotRecoverySource) s.primary().recoverySource();
        ShardId shardId = s.primary().shardId();

        RestoreInProgress completedRestore = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                source.restoreUUID(),
                s.snapshot(),
                RestoreInProgress.State.SUCCESS,
                false,
                List.of(shardId.getIndexName()),
                Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(s.primary().currentNodeId(), RestoreInProgress.State.SUCCESS))
            )
        ).build();

        assertFalse(RestoreService.isRestoringShard(completedRestore, s.primary()));
    }

    /**
     * Condition 6 variant: shard restore status is {@link RestoreInProgress.State#FAILURE} — must return {@code false}.
     * This case is real: {@link RestoreService} seeds {@code ignoreShards} entries with {@code State.FAILURE}
     * from the outset, so a live entry can carry already-completed shard statuses.
     */
    public void testIsRestoringShard_restoreStatusFailure_returnsFalse() {
        var s = buildRestoreTestState();
        SnapshotRecoverySource source = (SnapshotRecoverySource) s.primary().recoverySource();
        ShardId shardId = s.primary().shardId();

        RestoreInProgress failedRestore = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                source.restoreUUID(),
                s.snapshot(),
                RestoreInProgress.State.FAILURE,
                false,
                List.of(shardId.getIndexName()),
                Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(s.primary().currentNodeId(), RestoreInProgress.State.FAILURE))
            )
        ).build();

        assertFalse(RestoreService.isRestoringShard(failedRestore, s.primary()));
    }

    /**
     * Condition 6 variant: the restore's overall state is still {@link RestoreInProgress.State#STARTED} (other shards remain
     * in progress), but the specific shard under evaluation has already reached {@link RestoreInProgress.State#FAILURE}.
     * The predicate must return {@code false} based on the individual shard status, bypassing the early-exit on entry state.
     */
    public void testIsRestoringShard_shardStatusFailureRestoreStarted_returnsFalse() {
        var s = buildRestoreTestState();
        SnapshotRecoverySource source = (SnapshotRecoverySource) s.primary().recoverySource();
        ShardId shardId = s.primary().shardId();
        ShardId otherShardId = new ShardId(shardId.getIndexName(), shardId.getIndex().getUUID(), shardId.id() + 1);

        RestoreInProgress partiallyCompleteRestore = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                source.restoreUUID(),
                s.snapshot(),
                RestoreInProgress.State.STARTED,
                false,
                List.of(shardId.getIndexName()),
                Map.of(
                    shardId,
                    new RestoreInProgress.ShardRestoreStatus(s.primary().currentNodeId(), RestoreInProgress.State.FAILURE),
                    otherShardId,
                    new RestoreInProgress.ShardRestoreStatus(s.primary().currentNodeId(), RestoreInProgress.State.STARTED)
                )
            )
        ).build();

        assertFalse(RestoreService.isRestoringShard(partiallyCompleteRestore, s.primary()));
    }

    /**
     * Condition 6 variant: the entry's overall state is still {@link RestoreInProgress.State#STARTED} (other shards remain
     * in progress), but the specific shard under evaluation has already reached {@link RestoreInProgress.State#SUCCESS}.
     * The predicate must return {@code false} based on the individual shard status.
     */
    public void testIsRestoringShard_shardStatusSuccessRestoreStarted_returnsFalse() {
        var s = buildRestoreTestState();
        SnapshotRecoverySource source = (SnapshotRecoverySource) s.primary().recoverySource();
        ShardId shardId = s.primary().shardId();
        ShardId otherShardId = new ShardId(shardId.getIndexName(), shardId.getIndex().getUUID(), shardId.id() + 1);

        RestoreInProgress partiallyCompleteRestore = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                source.restoreUUID(),
                s.snapshot(),
                RestoreInProgress.State.STARTED,
                false,
                List.of(shardId.getIndexName()),
                Map.of(
                    shardId,
                    new RestoreInProgress.ShardRestoreStatus(s.primary().currentNodeId(), RestoreInProgress.State.SUCCESS),
                    otherShardId,
                    new RestoreInProgress.ShardRestoreStatus(s.primary().currentNodeId(), RestoreInProgress.State.STARTED)
                )
            )
        ).build();

        assertFalse(RestoreService.isRestoringShard(partiallyCompleteRestore, s.primary()));
    }

    /**
     * All conditions met with restore status {@link RestoreInProgress.State#INIT} — shard is initialising
     * before data transfer begins — predicate must return {@code true}.
     */
    public void testIsRestoringShard_shardStatusInit_returnsTrue() {
        var s = buildRestoreTestState();
        SnapshotRecoverySource source = (SnapshotRecoverySource) s.primary().recoverySource();
        ShardId shardId = s.primary().shardId();

        RestoreInProgress initRestore = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                source.restoreUUID(),
                s.snapshot(),
                RestoreInProgress.State.INIT,
                false,
                List.of(shardId.getIndexName()),
                Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(s.primary().currentNodeId(), RestoreInProgress.State.INIT))
            )
        ).build();

        assertTrue(RestoreService.isRestoringShard(initRestore, s.primary()));
    }

    // ---- restore-over-open-index guard tests ---------------------------------------------

    /**
     * A restore over an open index must refuse to publish the transition until every node in the cluster supports
     * {@link RecoveryFeatures#RESTORE_OVER_OPEN_INDEX_RECREATES_INDEX_SERVICE}, since a node without it cannot safely recreate the
     * {@code IndexService} for the resulting open-to-open history-UUID change.
     */
    public void testRestoreOverOpenIndexRejectsWhenNodeFeatureMissing() {
        final FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(RecoveryFeatures.RESTORE_OVER_OPEN_INDEX_RECREATES_INDEX_SERVICE))).thenReturn(
            false
        );
        final Snapshot snapshot = new Snapshot(ProjectId.DEFAULT, "test-repo", new SnapshotId("test-snap", randomUUID()));

        final SnapshotRestoreException e = expectThrows(
            SnapshotRestoreException.class,
            () -> RestoreService.ensureClusterSupportsRestoreOverOpenIndex(featureService, ClusterState.EMPTY_STATE, snapshot)
        );
        assertThat(e.getMessage(), containsString("not every node"));
    }

    /**
     * The caller resolves the exact destination {@link Index} (name and UUID) before submitting the restore, precisely so that an index
     * deleted and recreated under the same name is never silently adopted as the destination: the exact-identity check must reject a
     * resolved identity that no longer matches the index now present under that name.
     */
    public void testRestoreOverOpenIndexRejectsExactIdentityMismatch() {
        final IndexMetadata currentIndexMetadata = IndexMetadata.builder("test-idx")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .build();
        final Snapshot snapshot = new Snapshot(ProjectId.DEFAULT, "test-repo", new SnapshotId("test-snap", randomUUID()));
        // same name, different UUID: the identity the caller resolved is stale relative to the index now present
        final Index staleIndex = new Index(currentIndexMetadata.getIndex().getName(), UUIDs.randomBase64UUID());

        final SnapshotRestoreException e = expectThrows(
            SnapshotRestoreException.class,
            () -> RestoreService.validateExistingOpenIndexForRestore(
                snapshot,
                ClusterState.EMPTY_STATE,
                ProjectId.DEFAULT,
                currentIndexMetadata,
                currentIndexMetadata,
                staleIndex,
                false
            )
        );
        assertThat(e.getMessage(), containsString("no longer exists in the cluster state"));
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
