/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.MockLog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests the node-side service transition that an in-place restore over an already-open index requires: the destination index keeps its
 * index UUID and stays {@link IndexMetadata.State#OPEN} but receives a new history UUID, so every data node holding a copy must remove and
 * recreate its index service with reopened-index semantics rather than update it in place, while keeping the shard store on disk so that
 * the restore file diff can reuse identical local Lucene files.
 * <p>
 * The master-side atomic open-index restore operation does not exist yet, so {@link #initializeRestoreOverOpenIndex} publishes the
 * equivalent transition directly.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class RestoreOverOpenIndexIT extends AbstractSnapshotIntegTestCase {

    private static final String REPOSITORY_NAME = "test-repo";
    private static final String SNAPSHOT_NAME = "test-snap";
    private static final String INDEX_NAME = "test-idx";

    public void testRestoreOverOpenIndexReusesLocalFiles() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final int docCount = createRepositoryAndSnapshottedIndex();
        assertThat("a never-restored index has no history UUID", historyUuid(), nullValue());

        // Without the REOPENED transition, applying the restored metadata in place throws (IndexSettings rejects an in-place history
        // UUID change), which IndicesClusterStateService#updateIndices falls back to handling as an ordinary failed shard: it still ends
        // up reusing the on-disk store (IndexRemovalReason.FAILURE keeps it too) once the shard is retried, so that fallback path would
        // pass the assertions below even though it isn't the single clean transition this test means to verify. Assert directly that no
        // shard failure occurred, so a regression that disables the transition is caught here rather than silently masked.
        try (var mockLog = MockLog.capture(IndicesClusterStateService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no shard failure while applying the open-index restore transition",
                    IndicesClusterStateService.class.getName(),
                    Level.WARN,
                    "marking and sending shard failed"
                )
            );

            initializeRestoreOverOpenIndex();
            awaitRestoreCompleted();

            mockLog.assertAllExpectationsMatched();
        }

        assertThat("restore must assign a history UUID", historyUuid(), notNullValue());
        assertHitCount(prepareSearch(INDEX_NAME).setSize(0), docCount);

        // the index service was recreated as REOPENED rather than DELETED, so the shard store survived and the restore diff reused it
        final RecoveryState.Index recoveredIndex = restoreRecoveryState().getIndex();
        assertThat("restore should have reused the preserved local Lucene files", recoveredIndex.reusedFileCount(), greaterThan(0));
        assertThat("no file should have needed downloading again", recoveredIndex.recoveredFileCount(), equalTo(0));
    }

    public void testRestoredIndexSurvivesNodeRestart() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final int docCount = createRepositoryAndSnapshottedIndex();

        initializeRestoreOverOpenIndex();
        awaitRestoreCompleted();
        final String historyUuidAfterRestore = historyUuid();

        internalCluster().restartNode(dataNode);
        ensureGreen(INDEX_NAME);

        assertThat("the restored history UUID must survive the restart", historyUuid(), equalTo(historyUuidAfterRestore));
        assertHitCount(prepareSearch(INDEX_NAME).setSize(0), docCount);
    }

    public void testFailedRestoreOverOpenIndexPreservesTheStoreForARetry() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final int docCount = createRepositoryAndSnapshottedIndex();
        // resolve the snapshot before breaking the repository, since resolving it reads from the repository too
        final RestoreTarget restoreTarget = resolveRestoreTarget();

        try {
            // make every repository read fail, so the restore recovery of the recreated index service cannot succeed
            setControlIOExceptionRate(1.0);
            initializeRestoreOverOpenIndex(restoreTarget);

            // the transition is published regardless of the recovery outcome, and the shard exhausts its allocation retries
            assertThat("the transition is published before any recovery is attempted", historyUuid(), notNullValue());
            assertBusy(
                () -> assertThat(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, INDEX_NAME).get().getUnassignedShards(), greaterThan(0))
            );
        } finally {
            setControlIOExceptionRate(0.0);
        }

        // the failed attempt must not have discarded the local store, so retrying restores without downloading anything again
        ClusterRerouteUtils.rerouteRetryFailed(client());
        awaitRestoreCompleted();

        assertHitCount(prepareSearch(INDEX_NAME).setSize(0), docCount);
        assertThat(restoreRecoveryState().getIndex().reusedFileCount(), greaterThan(0));
    }

    private int createRepositoryAndSnapshottedIndex() throws Exception {
        createRepository(REPOSITORY_NAME, "mock");
        createIndex(INDEX_NAME, indexSettingsNoReplicas(1).build());

        final int docCount = randomIntBetween(20, 100);
        for (int i = 0; i < docCount; i++) {
            prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        // flush so that the snapshot and the surviving local store share the same committed segments
        indicesAdmin().prepareFlush(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);

        createFullSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME);
        return docCount;
    }

    /**
     * The identity of the snapshotted index to restore from, resolved by reading the repository. Resolving it is separate from publishing
     * the transition so that a test can break the repository in between.
     */
    private record RestoreTarget(Snapshot snapshot, IndexId indexId, IndexVersion indexVersion) {}

    private RestoreTarget resolveRestoreTarget() {
        final SnapshotInfo snapshotInfo = getSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME);
        return new RestoreTarget(
            new Snapshot(REPOSITORY_NAME, snapshotInfo.snapshotId()),
            getRepositoryData(REPOSITORY_NAME).resolveIndexId(INDEX_NAME),
            snapshotInfo.version()
        );
    }

    private void initializeRestoreOverOpenIndex() {
        initializeRestoreOverOpenIndex(resolveRestoreTarget());
    }

    /**
     * Drives the node-side transition under test: the data node holding the shard must apply, as a single change, an index that stays open
     * and keeps its index UUID but gains a new history UUID together with a restoring shard assigned to it.
     */
    private void initializeRestoreOverOpenIndex(RestoreTarget restoreTarget) {
        final ShardRouting startedPrimary = primaryShardRouting();
        assertThat(startedPrimary.state(), equalTo(ShardRoutingState.STARTED));
        safeGet(publishRestoreInitialization(restoreTarget, startedPrimary.currentNodeId()));
    }

    /**
     * Publishes, in a single cluster-state update, the transition that the master-side atomic open-index restore operation will publish:
     * the destination index keeps its index UUID and stays open, but receives a new history UUID, snapshot-recovery routing, rebuilt blocks
     * and a correlated {@link RestoreInProgress} entry.
     * <p>
     * The restoring primary is assigned to {@code shardNodeId}, the node that already holds the shard, within that same update. The
     * routing built by {@link RoutingTable.Builder#addAsRestore} leaves restored shards unassigned for a subsequent reroute to place; going
     * through that intermediate state would make the node drop its index service through the ordinary "no longer assigned" path and never
     * observe an open-to-open history UUID change. Assigning the shard up front produces the single-state transition under test, and reuses
     * the local store, which is the point of an in-place restore.
     *
     * @param shardNodeId the node that currently holds the started primary, and that the restoring primary is assigned to
     */
    private PlainActionFuture<Void> publishRestoreInitialization(RestoreTarget restoreTarget, String shardNodeId) {
        final String restoreUuid = UUIDs.randomBase64UUID();

        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final AllocationService allocationService = internalCluster().getCurrentMasterNodeInstance(AllocationService.class);
        final String localNodeId = clusterService.localNode().getId();

        final PlainActionFuture<Void> published = new PlainActionFuture<>();
        clusterService.submitUnbatchedStateUpdateTask("test: initialize restore over open index", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectId projectId = ProjectId.DEFAULT;
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final IndexMetadata currentIndexMetadata = project.index(INDEX_NAME);
                assertThat(currentIndexMetadata.getState(), equalTo(IndexMetadata.State.OPEN));

                // mirrors RestoreService#restoreOverClosedIndex: same index UUID, open, but a new history UUID
                final IndexMetadata restoredIndexMetadata = IndexMetadata.builder(currentIndexMetadata)
                    .settings(
                        Settings.builder()
                            .put(currentIndexMetadata.getSettings())
                            .put(IndexMetadata.SETTING_HISTORY_UUID, UUIDs.randomBase64UUID())
                    )
                    .settingsVersion(currentIndexMetadata.getSettingsVersion() + 1)
                    .timestampRange(IndexLongFieldRange.NO_SHARDS)
                    .eventIngestedRange(IndexLongFieldRange.NO_SHARDS)
                    .build();
                final Index index = restoredIndexMetadata.getIndex();

                final SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(
                    restoreUuid,
                    restoreTarget.snapshot(),
                    restoreTarget.indexVersion(),
                    restoreTarget.indexId()
                );
                final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards = new HashMap<>();
                for (int shard = 0; shard < restoredIndexMetadata.getNumberOfShards(); shard++) {
                    shards.put(new ShardId(index, shard), new RestoreInProgress.ShardRestoreStatus(localNodeId));
                }

                final ClusterState updatedState = ClusterState.builder(currentState)
                    .metadata(
                        Metadata.builder(currentState.metadata()).put(ProjectMetadata.builder(project).put(restoredIndexMetadata, true))
                    )
                    // rebuild the settings-derived blocks before anything else touches them, as ClusterBlocks.Builder#updateBlocks clears
                    // every existing block for the index
                    .blocks(ClusterBlocks.builder(currentState.blocks()).updateBlocks(projectId, restoredIndexMetadata))
                    .putRoutingTable(
                        projectId,
                        RoutingTable.builder(allocationService.getShardRoutingRoleStrategy(), currentState.routingTable(projectId))
                            .add(assignedRestoreRouting(restoredIndexMetadata, recoverySource, shardNodeId))
                            .build()
                    )
                    .putCustom(
                        RestoreInProgress.TYPE,
                        new RestoreInProgress.Builder(RestoreInProgress.get(currentState)).add(
                            new RestoreInProgress.Entry(
                                restoreUuid,
                                restoreTarget.snapshot(),
                                RestoreInProgress.State.INIT,
                                false,
                                List.of(INDEX_NAME),
                                Map.copyOf(shards)
                            )
                        ).build()
                    )
                    .build();

                return allocationService.reroute(updatedState, "test: restore over open index", ActionListener.noop());
            }

            @Override
            public void onFailure(Exception e) {
                published.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                published.onResponse(null);
            }
        });
        return published;
    }

    private void awaitRestoreCompleted() throws Exception {
        assertBusy(
            () -> assertThat(
                RestoreInProgress.get(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState()).isEmpty(),
                equalTo(true)
            )
        );
        ensureGreen(INDEX_NAME);
    }

    /**
     * @return snapshot-recovery routing for the index with every primary already initializing on {@code shardNodeId}
     */
    private static IndexRoutingTable assignedRestoreRouting(
        IndexMetadata restoredIndexMetadata,
        SnapshotRecoverySource recoverySource,
        String shardNodeId
    ) {
        final Index index = restoredIndexMetadata.getIndex();
        final IndexRoutingTable.Builder routing = IndexRoutingTable.builder(index);
        for (int shard = 0; shard < restoredIndexMetadata.getNumberOfShards(); shard++) {
            routing.addShard(
                ShardRouting.newUnassigned(
                    new ShardId(index, shard),
                    true,
                    recoverySource,
                    new UnassignedInfo(UnassignedInfo.Reason.EXISTING_INDEX_RESTORED, "restore over open index"),
                    ShardRouting.Role.DEFAULT,
                    ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
                ).initialize(shardNodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
            );
        }
        return routing.build();
    }

    private ShardRouting primaryShardRouting() {
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .routingTable(ProjectId.DEFAULT)
            .index(INDEX_NAME)
            .shard(0)
            .primaryShard();
    }

    /**
     * @return the index's current history UUID, or {@code null} if it has never been restored over
     */
    @Nullable
    private String historyUuid() {
        final ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        return state.metadata().getProject(ProjectId.DEFAULT).index(INDEX_NAME).getSettings().get(IndexMetadata.SETTING_HISTORY_UUID);
    }

    private static void setControlIOExceptionRate(double rate) {
        for (RepositoriesService repositoriesService : internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class)) {
            ((MockRepository) repositoriesService.repository(REPOSITORY_NAME)).setRandomControlIOExceptionRate(rate);
        }
    }

    private RecoveryState restoreRecoveryState() {
        final RecoveryResponse response = indicesAdmin().prepareRecoveries(INDEX_NAME).get();
        final List<RecoveryState> states = response.shardRecoveryStates()
            .get(INDEX_NAME)
            .stream()
            .filter(state -> state.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT)
            .toList();
        assertThat("expected exactly one snapshot recovery", states.size(), equalTo(1));
        return states.get(0);
    }
}
