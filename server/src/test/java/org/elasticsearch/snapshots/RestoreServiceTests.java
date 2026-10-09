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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.recovery.RecoveryFeatures;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
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

    // ---- isRestoringShardFromSnapshot predicate tests --------------------------------------------------

    /**
     * Bundles the objects needed to exercise {@link RestoreService#isRestoringShardFromSnapshot} with all six correlation
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
        return buildRestoreTestState(/* reportShardRestoring= */false);
    }

    private static RestoreTestState buildRestoreTestState(boolean reportShardRestoring) {
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
                Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(nodeId)),
                reportShardRestoring
            )
        ).build();

        return new RestoreTestState(primary, restoreInProgress, snapshot);
    }

    private static ClusterState clusterStateOf(RestoreTestState s) {
        return clusterStateOf(s.restoreInProgress(), IndexRoutingTable.builder(s.primary().index()).addShard(s.primary()));
    }

    private static ClusterState clusterStateOf(RestoreInProgress restoreInProgress, IndexRoutingTable.Builder... indices) {
        RoutingTable.Builder routingTable = RoutingTable.builder();
        for (IndexRoutingTable.Builder index : indices) {
            routingTable.add(index);
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(routingTable.build())
            .putCustom(RestoreInProgress.TYPE, restoreInProgress)
            .build();
    }

    private static void assertReportsRestore(ShardRestoringException e, RestoreTestState s, String restoreUuid) {
        assertNotNull(e);
        assertThat(e.recoveryId(), equalTo(restoreUuid));
        assertThat(e.getMetadata("es.index"), equalTo(List.of(s.primary().shardId().getIndexName())));
    }

    /**
     * A restore whose caller asked for its shards to be reported yields a {@link ShardRestoringException} that names the index and
     * carries the restore UUID as its recovery ID, both by shard and by index.
     */
    public void testReportableRestoreException_reportingRestore_returnsException() {
        var s = buildRestoreTestState(/* reportShardRestoring= */true);
        var state = clusterStateOf(s);
        String restoreUuid = ((SnapshotRecoverySource) s.primary().recoverySource()).restoreUUID();

        assertReportsRestore(RestoreService.reportableRestoreException(ProjectId.DEFAULT, s.primary().shardId(), state), s, restoreUuid);
        assertReportsRestore(RestoreService.reportableRestoreException(ProjectId.DEFAULT, s.primary().index(), state), s, restoreUuid);
    }

    /**
     * A restore whose caller did not ask for its shards to be reported yields {@code null} even though the shard is demonstrably
     * mid-restore, so callers keep the generic shard-unavailable errors.
     */
    public void testReportableRestoreException_nonReportingRestore_returnsNull() {
        var s = buildRestoreTestState(/* reportShardRestoring= */false);
        var state = clusterStateOf(s);
        assertTrue(RestoreService.isRestoringShardFromSnapshot(s.restoreInProgress(), s.primary()));

        assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, s.primary().shardId(), state));
        assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, s.primary().index(), state));
    }

    /**
     * A primary that is still waiting to be allocated is being restored just as much as one that is initializing, so a restore that
     * reports its shards is reported for an {@code UNASSIGNED} primary too.
     */
    public void testReportableRestoreException_unassignedPrimary_returnsException() {
        var s = buildRestoreTestState(true);
        var unassigned = new RestoreTestState(
            TestShardRouting.shardRoutingBuilder(s.primary().shardId(), null, true, ShardRoutingState.UNASSIGNED)
                .withRecoverySource(s.primary().recoverySource())
                .withUnassignedInfo(new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"))
                .build(),
            s.restoreInProgress(),
            s.snapshot()
        );
        var state = clusterStateOf(unassigned);
        String restoreUuid = ((SnapshotRecoverySource) unassigned.primary().recoverySource()).restoreUUID();

        assertReportsRestore(
            RestoreService.reportableRestoreException(ProjectId.DEFAULT, unassigned.primary().shardId(), state),
            unassigned,
            restoreUuid
        );
        assertReportsRestore(
            RestoreService.reportableRestoreException(ProjectId.DEFAULT, unassigned.primary().index(), state),
            unassigned,
            restoreUuid
        );
    }

    /**
     * An index that was deleted and recreated under the same name is a different index, so a restore of the old one must not be
     * reported for the new one.
     */
    public void testReportableRestoreException_sameNameDifferentIndexUuid_returnsNull() {
        var s = buildRestoreTestState(true);
        var state = clusterStateOf(s);
        var recreated = new Index(s.primary().index().getName(), randomUUID());

        assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, new ShardId(recreated, 0), state));
        assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, recreated, state));
    }

    /**
     * A shard number the index does not have is not being restored, and looking it up must not fail.
     */
    public void testReportableRestoreException_unknownShardNumber_returnsNull() {
        var s = buildRestoreTestState(true);
        var state = clusterStateOf(s);

        assertNull(
            RestoreService.reportableRestoreException(ProjectId.DEFAULT, new ShardId(s.primary().index(), randomIntBetween(1, 10)), state)
        );
    }

    /**
     * The shards of one index can each be in a different state, and only the shard that a reporting restore is still restoring is
     * reported. Shard 0 failed in the restore: it stays unassigned with its snapshot recovery source but the restore is no longer working
     * on it. Shard 1 is still being restored. Shard 2 has already started. Shard 3 is unavailable for a reason unrelated to the restore.
     * The shard-level lookup is exact, while the index-level lookup reports the index because one of its primaries is being restored.
     */
    public void testReportableRestoreException_shardsInDifferentStates_reportsOnlyTheShardStillBeingRestored() {
        var s = buildRestoreTestState(true);
        var index = s.primary().index();
        var restoreUuid = ((SnapshotRecoverySource) s.primary().recoverySource()).restoreUUID();
        var failedShard = new ShardId(index, 0);
        var restoringShard = new ShardId(index, 1);
        var startedShard = new ShardId(index, 2);
        var unrelatedShard = new ShardId(index, 3);
        var entry = s.restoreInProgress().get(restoreUuid);
        var running = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                entry.uuid(),
                entry.snapshot(),
                RestoreInProgress.State.STARTED,
                entry.quiet(),
                entry.indices(),
                Map.of(
                    failedShard,
                    new RestoreInProgress.ShardRestoreStatus(null, RestoreInProgress.State.FAILURE, "shard could not be allocated"),
                    restoringShard,
                    new RestoreInProgress.ShardRestoreStatus(s.primary().currentNodeId())
                ),
                true
            )
        ).build();
        var failedPrimary = TestShardRouting.shardRoutingBuilder(failedShard, null, true, ShardRoutingState.UNASSIGNED)
            .withRecoverySource(s.primary().recoverySource())
            .withUnassignedInfo(new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"))
            .build();
        var restoringPrimary = TestShardRouting.shardRoutingBuilder(
            restoringShard,
            s.primary().currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        ).withRecoverySource(s.primary().recoverySource()).build();
        var startedPrimary = TestShardRouting.shardRoutingBuilder(startedShard, randomUUID(), true, ShardRoutingState.STARTED).build();
        var unrelatedPrimary = TestShardRouting.shardRoutingBuilder(unrelatedShard, null, true, ShardRoutingState.UNASSIGNED)
            .withRecoverySource(RecoverySource.EmptyStoreRecoverySource.INSTANCE)
            .withUnassignedInfo(new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"))
            .build();
        var state = clusterStateOf(
            running,
            IndexRoutingTable.builder(index)
                .addShard(failedPrimary)
                .addShard(restoringPrimary)
                .addShard(startedPrimary)
                .addShard(unrelatedPrimary)
        );

        assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, failedShard, state));
        assertReportsRestore(RestoreService.reportableRestoreException(ProjectId.DEFAULT, restoringShard, state), s, restoreUuid);
        assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, startedShard, state));
        assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, unrelatedShard, state));
        assertReportsRestore(RestoreService.reportableRestoreException(ProjectId.DEFAULT, index, state), s, restoreUuid);
    }

    /**
     * Whether a shard is reported depends on the flag of the restore that is restoring that shard, not on any other restore that
     * happens to be in the cluster state.
     */
    public void testReportableRestoreException_twoRestores_usesTheFlagOfTheMatchingRestore() {
        var notReporting = buildRestoreTestState(false);
        var reporting = buildRestoreTestState(true);
        var restoreInProgressBuilder = new RestoreInProgress.Builder(notReporting.restoreInProgress());
        reporting.restoreInProgress().forEach(restoreInProgressBuilder::add);
        var state = clusterStateOf(
            restoreInProgressBuilder.build(),
            IndexRoutingTable.builder(notReporting.primary().index()).addShard(notReporting.primary()),
            IndexRoutingTable.builder(reporting.primary().index()).addShard(reporting.primary())
        );

        assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, notReporting.primary().shardId(), state));
        assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, notReporting.primary().index(), state));
        assertReportsRestore(
            RestoreService.reportableRestoreException(ProjectId.DEFAULT, reporting.primary().shardId(), state),
            reporting,
            ((SnapshotRecoverySource) reporting.primary().recoverySource()).restoreUUID()
        );
        assertReportsRestore(
            RestoreService.reportableRestoreException(ProjectId.DEFAULT, reporting.primary().index(), state),
            reporting,
            ((SnapshotRecoverySource) reporting.primary().recoverySource()).restoreUUID()
        );
    }

    /**
     * Deleting an index rebuilds the entries of restores that cover it. The rebuilt entry must keep the flag the restore's caller chose,
     * or a flagged restore would stop reporting its shards as soon as one of its indices was deleted.
     */
    public void testUpdateRestoreStateWithDeletedIndices_preservesReportShardRestoring() {
        var s = buildRestoreTestState(true);
        String restoreUuid = ((SnapshotRecoverySource) s.primary().recoverySource()).restoreUUID();

        RestoreInProgress updated = RestoreService.updateRestoreStateWithDeletedIndices(
            s.restoreInProgress(),
            Set.of(s.primary().shardId().getIndex())
        );

        RestoreInProgress.Entry entry = updated.get(restoreUuid);
        assertThat("the entry was rebuilt", entry.shards().get(s.primary().shardId()).state(), equalTo(RestoreInProgress.State.FAILURE));
        assertTrue(entry.reportShardRestoring());
    }

    /**
     * A shard of a restore starting rebuilds the restore's entry in the same cluster state update as the routing change. The rebuilt entry
     * must keep the flag the restore's caller chose, or a flagged restore would stop reporting its shards as soon as its first shard
     * changed state.
     */
    public void testRestoreInProgressUpdater_shardStarted_preservesReportShardRestoring() {
        var s = buildRestoreTestState(true);
        String restoreUuid = ((SnapshotRecoverySource) s.primary().recoverySource()).restoreUUID();

        var updater = new RestoreService.RestoreInProgressUpdater();
        updater.shardStarted(s.primary(), s.primary().moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE));
        RestoreInProgress updated = updater.applyChanges(s.restoreInProgress());

        RestoreInProgress.Entry entry = updated.get(restoreUuid);
        assertThat("the entry was rebuilt", entry.shards().get(s.primary().shardId()).state(), equalTo(RestoreInProgress.State.SUCCESS));
        assertTrue(entry.reportShardRestoring());
    }

    /**
     * Only shards whose restore has not completed block a new restore of the same index, so a restore that failed for a shard can still
     * have its entry in the cluster state when a restarted restore with a new UUID begins. Both entries then list the shard. The answer has
     * to follow the restore the shard's routing points at, not whichever entry happens to cover the shard.
     */
    public void testReportableRestoreException_restoreRestartedWithNewUuid_followsTheRoutingsRestore() {
        for (boolean restartedRestoreReports : new boolean[] { true, false }) {
            var restarted = buildRestoreTestState(restartedRestoreReports);
            var shardId = restarted.primary().shardId();
            var earlierRestore = new RestoreInProgress.Entry(
                randomUUID(),
                restarted.snapshot(),
                RestoreInProgress.State.FAILURE,
                false,
                List.of(shardId.getIndexName()),
                Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(null, RestoreInProgress.State.FAILURE, "restore failed")),
                restartedRestoreReports == false
            );
            var restoreInProgress = new RestoreInProgress.Builder(restarted.restoreInProgress()).add(earlierRestore).build();
            var state = clusterStateOf(new RestoreTestState(restarted.primary(), restoreInProgress, restarted.snapshot()));
            var restartedUuid = ((SnapshotRecoverySource) restarted.primary().recoverySource()).restoreUUID();

            if (restartedRestoreReports) {
                assertReportsRestore(
                    RestoreService.reportableRestoreException(ProjectId.DEFAULT, shardId, state),
                    restarted,
                    restartedUuid
                );
                assertReportsRestore(
                    RestoreService.reportableRestoreException(ProjectId.DEFAULT, restarted.primary().index(), state),
                    restarted,
                    restartedUuid
                );
            } else {
                assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, shardId, state));
                assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, restarted.primary().index(), state));
            }
        }
    }

    /**
     * A cluster state with no {@link RestoreInProgress} custom at all, as after a master failover, reports nothing. The case of an entry
     * missing from an existing custom is covered by the {@code isRestoringShardFromSnapshot} tests.
     */
    public void testReportableRestoreException_noRestoreInProgressCustom_returnsNull() {
        var s = buildRestoreTestState(true);
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(s.primary().index()).addShard(s.primary())).build())
            .build();

        assertNull(RestoreService.reportableRestoreException(ProjectId.DEFAULT, s.primary().shardId(), state));
    }

    /**
     * With several restores reporting their shards at once, each shard is reported with the UUID of the restore that is restoring it.
     */
    public void testReportableRestoreException_twoReportingRestores_eachReportsItsOwnUuid() {
        var first = buildRestoreTestState(true);
        var second = buildRestoreTestState(true);
        var restoreInProgressBuilder = new RestoreInProgress.Builder(first.restoreInProgress());
        second.restoreInProgress().forEach(restoreInProgressBuilder::add);
        var state = clusterStateOf(
            restoreInProgressBuilder.build(),
            IndexRoutingTable.builder(first.primary().index()).addShard(first.primary()),
            IndexRoutingTable.builder(second.primary().index()).addShard(second.primary())
        );
        var firstUuid = ((SnapshotRecoverySource) first.primary().recoverySource()).restoreUUID();
        var secondUuid = ((SnapshotRecoverySource) second.primary().recoverySource()).restoreUUID();
        assertThat(firstUuid, not(equalTo(secondUuid)));

        assertReportsRestore(
            RestoreService.reportableRestoreException(ProjectId.DEFAULT, first.primary().shardId(), state),
            first,
            firstUuid
        );
        assertReportsRestore(
            RestoreService.reportableRestoreException(ProjectId.DEFAULT, first.primary().index(), state),
            first,
            firstUuid
        );
        assertReportsRestore(
            RestoreService.reportableRestoreException(ProjectId.DEFAULT, second.primary().shardId(), state),
            second,
            secondUuid
        );
        assertReportsRestore(
            RestoreService.reportableRestoreException(ProjectId.DEFAULT, second.primary().index(), state),
            second,
            secondUuid
        );
    }

    /**
     * With several projects in the cluster state, each lookup reads only the routing table of the project it is given, so a shard is
     * reported under its own project and is not seen from another one. The deprecated {@code ClusterState#routingTable()} would throw here.
     */
    public void testReportableRestoreException_multipleProjects_readsOnlyTheRequestedProjectsRoutingTable() {
        var inFirstProject = buildRestoreTestState(true);
        var inSecondProject = buildRestoreTestState(true);
        var firstProject = randomUniqueProjectId();
        var secondProject = randomUniqueProjectId();
        var restoreInProgressBuilder = new RestoreInProgress.Builder(inFirstProject.restoreInProgress());
        inSecondProject.restoreInProgress().forEach(restoreInProgressBuilder::add);
        // a cluster state needs metadata for exactly the projects that have a routing table
        var metadata = Metadata.builder()
            .removeProject(ProjectId.DEFAULT)
            .put(ProjectMetadata.builder(firstProject))
            .put(ProjectMetadata.builder(secondProject))
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .putRoutingTable(
                firstProject,
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(inFirstProject.primary().index()).addShard(inFirstProject.primary()))
                    .build()
            )
            .putRoutingTable(
                secondProject,
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(inSecondProject.primary().index()).addShard(inSecondProject.primary()))
                    .build()
            )
            .putCustom(RestoreInProgress.TYPE, restoreInProgressBuilder.build())
            .build();
        var firstUuid = ((SnapshotRecoverySource) inFirstProject.primary().recoverySource()).restoreUUID();
        var secondUuid = ((SnapshotRecoverySource) inSecondProject.primary().recoverySource()).restoreUUID();

        assertReportsRestore(
            RestoreService.reportableRestoreException(firstProject, inFirstProject.primary().shardId(), state),
            inFirstProject,
            firstUuid
        );
        assertReportsRestore(
            RestoreService.reportableRestoreException(firstProject, inFirstProject.primary().index(), state),
            inFirstProject,
            firstUuid
        );
        assertReportsRestore(
            RestoreService.reportableRestoreException(secondProject, inSecondProject.primary().shardId(), state),
            inSecondProject,
            secondUuid
        );
        assertReportsRestore(
            RestoreService.reportableRestoreException(secondProject, inSecondProject.primary().index(), state),
            inSecondProject,
            secondUuid
        );

        // the other project's shards are not in this project's routing table
        assertNull(RestoreService.reportableRestoreException(firstProject, inSecondProject.primary().shardId(), state));
        assertNull(RestoreService.reportableRestoreException(firstProject, inSecondProject.primary().index(), state));
        assertNull(RestoreService.reportableRestoreException(secondProject, inFirstProject.primary().shardId(), state));
        assertNull(RestoreService.reportableRestoreException(secondProject, inFirstProject.primary().index(), state));
    }

    /**
     * Baseline: all six conditions met, restore status {@code STARTED} — predicate must return {@code true}.
     */
    public void testIsRestoringShard_allConditionsMet_returnsTrue() {
        var s = buildRestoreTestState();
        assertTrue(RestoreService.isRestoringShardFromSnapshot(s.restoreInProgress(), s.primary()));
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

        assertFalse(RestoreService.isRestoringShardFromSnapshot(s.restoreInProgress(), peerRecovery));
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

        assertFalse(RestoreService.isRestoringShardFromSnapshot(s.restoreInProgress(), noApiRouting));
    }

    /**
     * Condition 3: UUID present in the routing's recovery source has no matching entry in {@link RestoreInProgress}
     * (stale routing) — must return {@code false}.
     */
    public void testIsRestoringShard_staleUuid_returnsFalse() {
        var s = buildRestoreTestState();
        // EMPTY has no entries at all, so the UUID lookup returns null
        assertFalse(RestoreService.isRestoringShardFromSnapshot(RestoreInProgress.EMPTY, s.primary()));
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

        assertFalse(RestoreService.isRestoringShardFromSnapshot(unrelatedRestoreInProgress, s.primary()));
    }

    /**
     * Condition 4: an entry exists for the UUID but its {@link Snapshot} differs from the routing's recovery source
     * (mismatched correlation state). With assertions enabled this throws {@link AssertionError}; in production
     * (assertions disabled) it logs ERROR and returns {@code false}.
     */
    public void testIsRestoringShard_snapshotMismatch() {
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

        if (Assertions.ENABLED) {
            assertThrows(AssertionError.class, () -> RestoreService.isRestoringShardFromSnapshot(mismatchedRestore, s.primary()));
        } else {
            assertFalse(RestoreService.isRestoringShardFromSnapshot(mismatchedRestore, s.primary()));
        }
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

        assertFalse(RestoreService.isRestoringShardFromSnapshot(noShardRestore, s.primary()));
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

        assertFalse(RestoreService.isRestoringShardFromSnapshot(completedRestore, s.primary()));
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

        assertFalse(RestoreService.isRestoringShardFromSnapshot(failedRestore, s.primary()));
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

        assertFalse(RestoreService.isRestoringShardFromSnapshot(partiallyCompleteRestore, s.primary()));
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

        assertFalse(RestoreService.isRestoringShardFromSnapshot(partiallyCompleteRestore, s.primary()));
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

        assertTrue(RestoreService.isRestoringShardFromSnapshot(initRestore, s.primary()));
    }

    // ---- hook setter and executeRestoreCleanup tests -----------------

    /** Builds a minimal mocked {@link RestoreService} so instance methods can be exercised without a full cluster. */
    private RestoreService createMinimalRestoreService() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(ThreadPool.Names.SNAPSHOT_META)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        return new RestoreService(
            clusterService,
            mock(RepositoriesService.class),
            mock(AllocationService.class),
            mock(MetadataCreateIndexService.class),
            mock(IndexMetadataVerifier.class),
            mock(ShardLimitValidator.class),
            mock(SystemIndices.class),
            mock(IndicesService.class),
            mock(FileSettingsService.class),
            threadPool,
            false,
            IndexMetadataRestoreTransformer.NoOpRestoreTransformer.getInstance(),
            mock(FeatureService.class)
        );
    }

    /** Builds a {@link ClusterState} that contains one completed {@link RestoreInProgress} entry. */
    private static ClusterState stateWithCompletedRestore() {
        String nodeId = randomUUID();
        ShardId shardId = new ShardId(randomIdentifier(), randomUUID(), 0);
        Snapshot snapshot = new Snapshot(randomProjectIdOrDefault(), randomIdentifier(), new SnapshotId(randomIdentifier(), randomUUID()));
        RestoreInProgress rip = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                UUIDs.randomBase64UUID(),
                snapshot,
                RestoreInProgress.State.SUCCESS,
                false,
                List.of(shardId.getIndexName()),
                Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(nodeId, RestoreInProgress.State.SUCCESS))
            )
        ).build();
        return ClusterState.builder(ClusterState.EMPTY_STATE).putCustom(RestoreInProgress.TYPE, rip).build();
    }

    /** Builds a {@link ClusterState} that contains one still-active {@link RestoreInProgress} entry. */
    private static ClusterState stateWithActiveRestore() {
        String nodeId = randomUUID();
        ShardId shardId = new ShardId(randomIdentifier(), randomUUID(), 0);
        Snapshot snapshot = new Snapshot(randomProjectIdOrDefault(), randomIdentifier(), new SnapshotId(randomIdentifier(), randomUUID()));
        RestoreInProgress rip = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                UUIDs.randomBase64UUID(),
                snapshot,
                RestoreInProgress.State.STARTED,
                false,
                List.of(shardId.getIndexName()),
                Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(nodeId))
            )
        ).build();
        return ClusterState.builder(ClusterState.EMPTY_STATE).putCustom(RestoreInProgress.TYPE, rip).build();
    }

    /** Builds a listener that applies {@code onCompleted} on completion and defaults on initialization. */
    private static RestoreLifecycleListener completionListener(
        BiFunction<RestoreInProgress.Entry, ClusterState, ClusterState> onCompleted
    ) {
        return new RestoreLifecycleListener() {
            @Override
            public ClusterState onRestoreCompleted(RestoreInProgress.Entry entry, ClusterState state) {
                return onCompleted.apply(entry, state);
            }
        };
    }

    public void testSetLifecycleListenerRejectsNull() {
        RestoreService service = createMinimalRestoreService();
        expectThrows(NullPointerException.class, () -> service.setLifecycleListener(null));
    }

    public void testRestoreSnapshotRejectsInvalidRestoreUUID() {
        RestoreService service = createMinimalRestoreService();
        RestoreSnapshotRequest request = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT);
        expectThrows(
            IllegalArgumentException.class,
            () -> service.restoreSnapshot(
                randomProjectIdOrDefault(),
                request,
                SnapshotRecoverySource.NO_API_RESTORE_UUID,
                ActionListener.noop(),
                (clusterState, builder) -> {}
            )
        );
        expectThrows(
            NullPointerException.class,
            () -> service.restoreSnapshot(randomProjectIdOrDefault(), request, null, ActionListener.noop(), (clusterState, builder) -> {})
        );
    }

    public void testOnRestoreCompletedCalledForCompletedEntry() {
        RestoreService service = createMinimalRestoreService();
        AtomicInteger callCount = new AtomicInteger(0);
        service.setLifecycleListener(completionListener((entry, state) -> {
            callCount.incrementAndGet();
            return state;
        }));

        ClusterState result = service.executeRestoreCleanup(stateWithCompletedRestore());

        assertEquals(1, callCount.get());
        assertTrue(RestoreInProgress.get(result).isEmpty());
    }

    public void testOnRestoreCompletedNotCalledForActiveEntry() {
        RestoreService service = createMinimalRestoreService();
        AtomicInteger callCount = new AtomicInteger(0);
        service.setLifecycleListener(completionListener((entry, state) -> {
            callCount.incrementAndGet();
            return state;
        }));

        ClusterState input = stateWithActiveRestore();
        ClusterState result = service.executeRestoreCleanup(input);

        assertEquals(0, callCount.get());
        assertSame(input, result);
    }

    public void testDefaultNoopListenerPreservesClusterState() {
        RestoreService service = createMinimalRestoreService();

        ClusterState withActive = stateWithActiveRestore();
        assertSame(withActive, service.executeRestoreCleanup(withActive));

        ClusterState withCompleted = stateWithCompletedRestore();
        ClusterState cleaned = service.executeRestoreCleanup(withCompleted);
        assertTrue(RestoreInProgress.get(cleaned).isEmpty());
    }

    /**
     * Cleaning up completed restores leaves the other entries as they are, so a restore that is still running keeps asking for its shards
     * to be reported.
     */
    public void testExecuteRestoreCleanup_keepsReportShardRestoringOfTheEntriesItLeaves() {
        var running = buildRestoreTestState(true);
        var runningUuid = ((SnapshotRecoverySource) running.primary().recoverySource()).restoreUUID();
        var completedShard = new ShardId(randomIdentifier(), randomUUID(), 0);
        var completed = new RestoreInProgress.Entry(
            randomUUID(),
            running.snapshot(),
            RestoreInProgress.State.SUCCESS,
            false,
            List.of(completedShard.getIndexName()),
            Map.of(completedShard, new RestoreInProgress.ShardRestoreStatus(randomUUID(), RestoreInProgress.State.SUCCESS)),
            randomBoolean()
        );
        var restoreInProgress = new RestoreInProgress.Builder(running.restoreInProgress()).add(completed).build();
        var state = ClusterState.builder(ClusterState.EMPTY_STATE).putCustom(RestoreInProgress.TYPE, restoreInProgress).build();

        var cleaned = RestoreInProgress.get(createMinimalRestoreService().executeRestoreCleanup(state));

        assertThat("only the completed entry is removed", Iterables.size(cleaned), equalTo(1L));
        assertTrue(cleaned.get(runningUuid).reportShardRestoring());
    }

    public void testOnRestoreCompletedReceivesEntryAndCanModifyState() {
        RestoreService service = createMinimalRestoreService();
        List<RestoreInProgress.Entry> capturedEntries = new ArrayList<>();
        service.setLifecycleListener(completionListener((entry, state) -> {
            capturedEntries.add(entry);
            return ClusterState.builder(state).version(state.version() + 1).build();
        }));

        ClusterState input = stateWithCompletedRestore();
        RestoreInProgress.Entry expectedEntry = RestoreInProgress.get(input).iterator().next();
        long initialVersion = input.version();

        ClusterState result = service.executeRestoreCleanup(input);

        assertEquals(List.of(expectedEntry), capturedEntries);
        assertEquals(initialVersion + 1, result.version());
        assertTrue(RestoreInProgress.get(result).isEmpty());
    }

    /**
     * Verifies the retry contract documented on {@link RestoreLifecycleListener#onRestoreCompleted}: when
     * the listener throws, the entry is retained in cluster state and the listener is called again on the
     * next cleanup pass.
     */
    public void testOnRestoreCompletedRetriedAfterException() {
        RestoreService service = createMinimalRestoreService();
        AtomicInteger callCount = new AtomicInteger(0);
        RuntimeException boom = new RuntimeException("transient failure");
        service.setLifecycleListener(completionListener((entry, state) -> {
            if (callCount.incrementAndGet() == 1) {
                throw boom;
            }
            return state;
        }));

        ClusterState input = stateWithCompletedRestore();

        // First pass: listener throws — entry must be retained.
        ClusterState afterFirst = service.executeRestoreCleanup(input);
        assertEquals(1, callCount.get());
        assertFalse("entry must be retained after listener exception", RestoreInProgress.get(afterFirst).isEmpty());

        // Second pass: listener succeeds — entry must be removed.
        ClusterState afterSecond = service.executeRestoreCleanup(afterFirst);
        assertEquals(2, callCount.get());
        assertTrue("entry must be removed after successful listener call", RestoreInProgress.get(afterSecond).isEmpty());
    }

    // ---- applyRestoreInitializedListener tests --------------------------------

    /** Builds a single {@link RestoreInProgress.Entry} for use in initialization tests. */
    private static RestoreInProgress.Entry buildRestoreEntry() {
        String nodeId = randomUUID();
        ShardId shardId = new ShardId(randomIdentifier(), randomUUID(), 0);
        Snapshot snapshot = new Snapshot(randomProjectIdOrDefault(), randomIdentifier(), new SnapshotId(randomIdentifier(), randomUUID()));
        return new RestoreInProgress.Entry(
            UUIDs.randomBase64UUID(),
            snapshot,
            RestoreInProgress.State.INIT,
            false,
            List.of(shardId.getIndexName()),
            Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(nodeId))
        );
    }

    public void testApplyRestoreInitializedListenerCallsListenerWhenEntryNonNull() {
        RestoreService service = createMinimalRestoreService();
        AtomicInteger callCount = new AtomicInteger(0);
        service.setLifecycleListener(new RestoreLifecycleListener() {
            @Override
            public ClusterState onRestoreInitialized(RestoreInProgress.Entry entry, ClusterState state) {
                callCount.incrementAndGet();
                return ClusterState.builder(state).version(state.version() + 1).build();
            }
        });

        RestoreInProgress.Entry entry = buildRestoreEntry();
        long initialVersion = ClusterState.EMPTY_STATE.version();
        ClusterState result = service.applyRestoreInitializedListener(entry, ClusterState.EMPTY_STATE);

        assertEquals(1, callCount.get());
        assertEquals(initialVersion + 1, result.version());
    }

    public void testApplyRestoreInitializedListenerSkipsListenerWhenEntryNull() {
        RestoreService service = createMinimalRestoreService();
        AtomicInteger callCount = new AtomicInteger(0);
        service.setLifecycleListener(new RestoreLifecycleListener() {
            @Override
            public ClusterState onRestoreInitialized(RestoreInProgress.Entry entry, ClusterState state) {
                callCount.incrementAndGet();
                return state;
            }
        });

        ClusterState result = service.applyRestoreInitializedListener(null, ClusterState.EMPTY_STATE);

        assertEquals(0, callCount.get());
        assertSame(ClusterState.EMPTY_STATE, result);
    }

    public void testSetLifecycleListenerRejectsDoubleRegistration() {
        RestoreService service = createMinimalRestoreService();
        RestoreLifecycleListener realListener = new RestoreLifecycleListener() {};
        service.setLifecycleListener(realListener);
        expectThrows(IllegalStateException.class, () -> service.setLifecycleListener(realListener));
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

    /**
     * The caller resolves the destination as open, but it can be closed by a concurrent operation before this cluster-state update is
     * published (closing keeps the same index UUID, so the exact-identity check still passes). The open-index restore path assumes an
     * open-to-open transition, so it must reject a destination that is no longer open rather than proceed, and this is enforced at runtime
     * (not merely asserted) so the guarantee holds in production where assertions are disabled.
     */
    public void testRestoreOverOpenIndexRejectsIndexThatIsNoLongerOpen() {
        final IndexMetadata closedIndexMetadata = IndexMetadata.builder("test-idx")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .state(IndexMetadata.State.CLOSE)
            .build();
        final Snapshot snapshot = new Snapshot(ProjectId.DEFAULT, "test-repo", new SnapshotId("test-snap", randomUUID()));

        final SnapshotRestoreException e = expectThrows(
            SnapshotRestoreException.class,
            () -> RestoreService.validateExistingOpenIndexForRestore(
                snapshot,
                ClusterState.EMPTY_STATE,
                ProjectId.DEFAULT,
                closedIndexMetadata,
                closedIndexMetadata,
                closedIndexMetadata.getIndex(),
                false
            )
        );
        assertThat(e.getMessage(), containsString("no longer open"));
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

    /**
     * Tests that calling restoreSnapshotOverOpenIndices a second time with the same restoreUUID is a no-op.
     */
    public void testRestoreOverOpenIndicesIdempotentRetryIsANoOp() throws Exception {
        final String restoreUUID = UUIDs.randomBase64UUID();
        withOpenIndexRestoreHarness(fixture -> {
            PlainActionFuture<RestoreService.RestoreCompletionResponse> first = new PlainActionFuture<>();
            fixture.restoreService()
                .restoreSnapshotOverOpenIndices(
                    ProjectId.DEFAULT,
                    fixture.snapshot(),
                    fixture.snapshotInfo(),
                    TEST_REQUEST_TIMEOUT,
                    restoreUUID,
                    List.of(fixture.target()),
                    first
                );
            first.actionGet(TimeValue.timeValueSeconds(10));

            final ClusterState afterFirstCall = fixture.clusterService().state();
            final String historyUuidAfterFirstCall = historyUuid(afterFirstCall, fixture.index().getName());
            assertThat(historyUuidAfterFirstCall, notNullValue());
            assertThat(Iterables.size(RestoreInProgress.get(afterFirstCall)), equalTo(1L));

            PlainActionFuture<RestoreService.RestoreCompletionResponse> second = new PlainActionFuture<>();
            fixture.restoreService()
                .restoreSnapshotOverOpenIndices(
                    ProjectId.DEFAULT,
                    fixture.snapshot(),
                    fixture.snapshotInfo(),
                    TEST_REQUEST_TIMEOUT,
                    restoreUUID,
                    List.of(fixture.target()),
                    second
                );
            second.actionGet(TimeValue.timeValueSeconds(10));

            final ClusterState afterSecondCall = fixture.clusterService().state();
            assertThat(historyUuid(afterSecondCall, fixture.index().getName()), equalTo(historyUuidAfterFirstCall));
            assertThat(Iterables.size(RestoreInProgress.get(afterSecondCall)), equalTo(1L));
        });
    }

    /**
     * This test shows that {@link RestoreService#restoreSnapshotOverOpenIndices} is not idempotent across completed restores. It protects
     * against two in-flight restores at the same time only.It holds only while the first restore's {@link RestoreInProgress} entry exists.
     * That entry is transient ({@code removeCompletedRestoresFromClusterState} removes it once the restore completes). And because
     * restoring over an open index preserves the destination's index UUID, the exact-identity check still passes on a retry after the entry
     * is gone. So a same-{@code restoreUUID} retry is <em>not</em> deduplicated once cleaned up. It starts a fresh restore and creates a
     * new history UUID. Guaranteeing at-most-once across the full lifecycle is the caller's responsibility (see
     * {@link RestoreService#restoreSnapshotOverOpenIndices}).
     */
    public void testRestoreOverOpenIndicesRetryAfterCompletionIsNotDeduplicated() throws Exception {
        final String restoreUUID = UUIDs.randomBase64UUID();
        withOpenIndexRestoreHarness(fixture -> {
            PlainActionFuture<RestoreService.RestoreCompletionResponse> first = new PlainActionFuture<>();
            fixture.restoreService()
                .restoreSnapshotOverOpenIndices(
                    ProjectId.DEFAULT,
                    fixture.snapshot(),
                    fixture.snapshotInfo(),
                    TEST_REQUEST_TIMEOUT,
                    restoreUUID,
                    List.of(fixture.target()),
                    first
                );
            first.actionGet(TimeValue.timeValueSeconds(10));

            final ClusterState afterFirstCall = fixture.clusterService().state();
            final String historyUuidAfterFirstCall = historyUuid(afterFirstCall, fixture.index().getName());
            assertThat(historyUuidAfterFirstCall, notNullValue());
            assertThat(Iterables.size(RestoreInProgress.get(afterFirstCall)), equalTo(1L));

            // Simulate the completed-restore cleanup that removeCompletedRestoresFromClusterState() performs once the restore finishes. The
            // harness's restore never truly completes (routing is mocked), so strip the entry directly to reach the post-cleanup state.
            ClusterServiceUtils.setState(
                fixture.clusterService(),
                ClusterState.builder(afterFirstCall).putCustom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).build()
            );
            assertThat(Iterables.size(RestoreInProgress.get(fixture.clusterService().state())), equalTo(0L));

            PlainActionFuture<RestoreService.RestoreCompletionResponse> second = new PlainActionFuture<>();
            fixture.restoreService()
                .restoreSnapshotOverOpenIndices(
                    ProjectId.DEFAULT,
                    fixture.snapshot(),
                    fixture.snapshotInfo(),
                    TEST_REQUEST_TIMEOUT,
                    restoreUUID,
                    List.of(fixture.target()),
                    second
                );
            second.actionGet(TimeValue.timeValueSeconds(10));

            // The retry was not deduplicated: a fresh restore was initialized, minting a new history UUID and installing a new entry.
            final ClusterState afterRetry = fixture.clusterService().state();
            assertThat(historyUuid(afterRetry, fixture.index().getName()), not(equalTo(historyUuidAfterFirstCall)));
            assertThat(Iterables.size(RestoreInProgress.get(afterRetry)), equalTo(1L));
        });
    }

    /**
     * A retry with the same restore UUID while the first restore's entry still exists applies nothing, so it cannot change the flag the
     * first call set, whichever way the retry asks.
     */
    public void testRestoreOverOpenIndicesRetryWithOppositeFlagKeepsTheFirstCallsFlag() throws Exception {
        for (boolean firstCallReports : new boolean[] { true, false }) {
            final String restoreUUID = UUIDs.randomBase64UUID();
            withOpenIndexRestoreHarness(fixture -> {
                restoreOverOpenIndices(fixture, restoreUUID, firstCallReports);
                restoreOverOpenIndices(fixture, restoreUUID, firstCallReports == false);

                final RestoreInProgress restoreInProgress = RestoreInProgress.get(fixture.clusterService().state());
                assertThat(Iterables.size(restoreInProgress), equalTo(1L));
                assertThat(restoreInProgress.get(restoreUUID).reportShardRestoring(), equalTo(firstCallReports));
            });
        }
    }

    /**
     * The flag lives only on the restore's entry. Once that entry is gone, for example because it was lost in a master failover, a
     * resubmission with the same restore UUID starts a fresh restore whose flag is the one the new call passes, not the one the earlier
     * restore had. A caller that wants its shards reported has to pass the flag every time it submits.
     */
    public void testRestoreOverOpenIndicesAfterTheEntryIsGoneTheNewCallsFlagApplies() throws Exception {
        for (boolean firstCallReports : new boolean[] { true, false }) {
            final String restoreUUID = UUIDs.randomBase64UUID();
            withOpenIndexRestoreHarness(fixture -> {
                restoreOverOpenIndices(fixture, restoreUUID, firstCallReports);

                // As in testRestoreOverOpenIndicesRetryAfterCompletionIsNotDeduplicated, strip the entry to reach the state after cleanup.
                ClusterServiceUtils.setState(
                    fixture.clusterService(),
                    ClusterState.builder(fixture.clusterService().state())
                        .putCustom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)
                        .build()
                );
                restoreOverOpenIndices(fixture, restoreUUID, firstCallReports == false);

                final RestoreInProgress restoreInProgress = RestoreInProgress.get(fixture.clusterService().state());
                assertThat(Iterables.size(restoreInProgress), equalTo(1L));
                assertThat(restoreInProgress.get(restoreUUID).reportShardRestoring(), equalTo(firstCallReports == false));
            });
        }
    }

    /**
     * An empty {@code targets} list is a caller error for this internal entry point. It is rejected up front rather than submitting a
     * restore that does nothing, which was probably not the user's intention.
     */
    public void testRestoreOverOpenIndicesRejectsEmptyTargets() throws Exception {
        withOpenIndexRestoreHarness(fixture -> {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> fixture.restoreService()
                    .restoreSnapshotOverOpenIndices(
                        ProjectId.DEFAULT,
                        fixture.snapshot(),
                        fixture.snapshotInfo(),
                        TEST_REQUEST_TIMEOUT,
                        UUIDs.randomBase64UUID(),
                        List.of(),
                        ActionListener.noop()
                    )
            );
            assertThat(e.getMessage(), equalTo("targets must not be empty"));
            assertThat(
                "a rejected call must not initialize a restore",
                Iterables.size(RestoreInProgress.get(fixture.clusterService().state())),
                equalTo(0L)
            );
        });
    }

    /**
     * A restore whose snapshot names a project that does not exist in the cluster state is rejected when the cluster-state update runs,
     * surfacing the failure through the listener rather than mutating anything.
     */
    public void testRestoreOverOpenIndicesRejectsMissingProject() throws Exception {
        withOpenIndexRestoreHarness(fixture -> {
            final ProjectId missingProject = ProjectId.fromId("does-not-exist");
            final Snapshot snapshotInMissingProject = new Snapshot(
                missingProject,
                fixture.snapshot().getRepository(),
                fixture.snapshot().getSnapshotId()
            );
            final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = new PlainActionFuture<>();
            fixture.restoreService()
                .restoreSnapshotOverOpenIndices(
                    missingProject,
                    snapshotInMissingProject,
                    fixture.snapshotInfo(),
                    TEST_REQUEST_TIMEOUT,
                    UUIDs.randomBase64UUID(),
                    List.of(fixture.target()),
                    future
                );
            final SnapshotRestoreException e = expectThrows(
                SnapshotRestoreException.class,
                () -> future.actionGet(TimeValue.timeValueSeconds(10))
            );
            assertThat(e.getMessage(), containsString("project [" + missingProject + "] does not exist"));
            assertThat(
                "a rejected restore must not initialize an entry",
                Iterables.size(RestoreInProgress.get(fixture.clusterService().state())),
                equalTo(0L)
            );
        });
    }

    private static String historyUuid(ClusterState state, String indexName) {
        return state.metadata().getProject(ProjectId.DEFAULT).index(indexName).getSettings().get(IndexMetadata.SETTING_HISTORY_UUID);
    }

    private record OpenIndexRestoreFixture(
        RestoreService restoreService,
        ClusterService clusterService,
        Index index,
        Snapshot snapshot,
        SnapshotInfo snapshotInfo,
        IndexId snapshotIndexId,
        IndexMetadata snapshotIndexMetadata
    ) {
        RestoreService.OpenIndexRestoreTarget target() {
            return target(index);
        }

        RestoreService.OpenIndexRestoreTarget target(Index destinationIndex) {
            return new RestoreService.OpenIndexRestoreTarget(destinationIndex, snapshotIndexId, snapshotIndexMetadata);
        }
    }

    private interface OpenIndexRestoreTestBody {
        void run(OpenIndexRestoreFixture fixture) throws Exception;
    }

    private static void restoreOverOpenIndices(OpenIndexRestoreFixture fixture, String restoreUUID, boolean reportShardRestoring) {
        final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = new PlainActionFuture<>();
        fixture.restoreService()
            .restoreSnapshotOverOpenIndices(
                ProjectId.DEFAULT,
                fixture.snapshot(),
                fixture.snapshotInfo(),
                TEST_REQUEST_TIMEOUT,
                restoreUUID,
                reportShardRestoring,
                List.of(fixture.target()),
                future
            );
        future.actionGet(TimeValue.timeValueSeconds(10));
    }

    /**
     * Builds a real, single-node {@link ClusterService} (via {@link ClusterServiceUtils}) with one open index, and a {@link RestoreService}
     * wired to it. Dependencies that the open-index restore validation path never reaches (index creation, mapping/version verification
     * beyond a pass-through, shard limits, system indices, file settings) are mocked or stubbed with no-ops. Constructing the real
     * equivalents would require an unrelated mapper/x-content registry setup this test does not exercise.
     */
    private void withOpenIndexRestoreHarness(OpenIndexRestoreTestBody body) throws Exception {
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool)) {
            final ClusterState initial = clusterService.state();
            final DiscoveryNode localNode = initial.nodes().getLocalNode();

            final IndexMetadata indexMetadata = IndexMetadata.builder("test-idx")
                .settings(indexSettings(IndexVersion.current(), 1, 0))
                .build();
            final Index index = indexMetadata.getIndex();

            final IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index);
            indexRoutingTable.addShard(
                TestShardRouting.newShardRouting(new ShardId(index, 0), localNode.getId(), true, ShardRoutingState.STARTED)
            );

            final ClusterState state = ClusterState.builder(initial)
                .putProjectMetadata(ProjectMetadata.builder(initial.metadata().getProject(ProjectId.DEFAULT)).put(indexMetadata, true))
                .putRoutingTable(
                    ProjectId.DEFAULT,
                    RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, initial.routingTable(ProjectId.DEFAULT))
                        .add(indexRoutingTable)
                        .build()
                )
                .build();
            ClusterServiceUtils.setState(clusterService, state);

            final RepositoriesService repositoriesService = mock(RepositoriesService.class);
            when(repositoriesService.getPreRestoreVersionChecks()).thenReturn(List.of());

            final AllocationService allocationService = mock(AllocationService.class);
            when(allocationService.getShardRoutingRoleStrategy()).thenReturn(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
            when(allocationService.reroute(any(), any(), any())).thenAnswer(invocation -> {
                ActionListener<Void> rerouteListener = invocation.getArgument(2);
                rerouteListener.onResponse(null);
                return invocation.getArgument(0);
            });

            final IndexMetadataVerifier indexMetadataVerifier = mock(IndexMetadataVerifier.class);
            when(indexMetadataVerifier.verifyIndexMetadata(any(), any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

            // the open-index restore path checks this feature at publish time; the harness exercises the happy path, so advertise support
            final FeatureService featureService = mock(FeatureService.class);
            when(featureService.clusterHasFeature(any(), eq(RecoveryFeatures.RESTORE_OVER_OPEN_INDEX_RECREATES_INDEX_SERVICE))).thenReturn(
                true
            );

            final RestoreService restoreService = new RestoreService(
                clusterService,
                repositoriesService,
                allocationService,
                mock(MetadataCreateIndexService.class),
                indexMetadataVerifier,
                mock(ShardLimitValidator.class),
                EmptySystemIndices.INSTANCE,
                mock(IndicesService.class),
                mock(FileSettingsService.class),
                threadPool,
                false,
                IndexMetadataRestoreTransformer.NoOpRestoreTransformer.getInstance(),
                featureService
            );

            final Snapshot snapshot = new Snapshot(ProjectId.DEFAULT, "test-repo", new SnapshotId("test-snap", randomUUID()));
            final SnapshotInfo snapshotInfo = createSnapshotInfo(snapshot, Boolean.FALSE);
            final IndexId snapshotIndexId = new IndexId(index.getName(), randomUUID());

            body.run(
                new OpenIndexRestoreFixture(restoreService, clusterService, index, snapshot, snapshotInfo, snapshotIndexId, indexMetadata)
            );
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * This tests that a restore over an open index that is being resharded is rejected. Restoring while resharding is happening would fail.
     * Plus, you can't close an index that is resharding, so we are not losing any functionality a user had previously by explicitly closing
     * an index and then restoring.
     */
    public void testRestoreOverOpenIndexRejectsReshardingIndex() {
        final IndexMetadata currentIndexMetadata = IndexMetadata.builder("test-idx")
            .settings(indexSettings(IndexVersion.current(), 2, 0))
            .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(2, 2))
            .build();
        final Index index = currentIndexMetadata.getIndex();
        final ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).put(currentIndexMetadata, false))
            .build();
        final Snapshot snapshot = new Snapshot(ProjectId.DEFAULT, "test-repo", new SnapshotId("test-snap", randomUUID()));

        final SnapshotRestoreException e = expectThrows(
            SnapshotRestoreException.class,
            () -> RestoreService.validateExistingOpenIndexForRestore(
                snapshot,
                state,
                ProjectId.DEFAULT,
                currentIndexMetadata,
                currentIndexMetadata,
                index,
                false
            )
        );
        assertThat(e.getMessage(), containsString("being resharded"));
    }
}
