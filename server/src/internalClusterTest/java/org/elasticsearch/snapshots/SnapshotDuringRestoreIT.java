/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for the interaction between snapshots and in-progress shard restores.
 * <p>
 * When a shard's primary is {@code INITIALIZING} because it is being restored from a snapshot,
 * a new {@code partial=true} snapshot should record that shard as {@link SnapshotsInProgress.ShardState#MISSING}
 * (with reason {@link SnapshotsService#SHARD_BEING_RESTORED_REASON}) and complete promptly as
 * {@link SnapshotState#PARTIAL}, rather than waiting indefinitely for the restore to finish.
 * <p>
 * A {@code partial=false} snapshot must preserve the existing behaviour: it waits for the restore
 * to complete and then captures the shard, completing as {@link SnapshotState#SUCCESS}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotDuringRestoreIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class);
    }

    /**
     * A {@code partial=true} snapshot taken while a shard is being restored records the restoring
     * shard as {@link SnapshotsInProgress.ShardState#MISSING} and completes as
     * {@link SnapshotState#PARTIAL}. The shard failure carries {@link SnapshotsService#SHARD_BEING_RESTORED_REASON}.
     */
    public void testPartialSnapshotWhileRestoringCompletesAsPartial() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final String indexName = "test-index";
        final String repoName = "test-repo";
        final String sourceSnapshot = "source-snapshot";
        createIndexWithContent(indexName);
        createRepository(repoName, "mock");
        createFullSnapshot(repoName, sourceSnapshot);

        // Delete rather than close: a closed index retains shard data on disk, so the data node skips
        // repo reads during recovery. Deleting forces chunk-blob reads from the repository, which means
        // blockAllDataNodes actually holds the primary in INITIALIZING.
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Hold the restore in INITIALIZING by blocking the data node's repo I/O.
        blockAllDataNodes(repoName);
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot)
            .setIndices(indexName)
            .setWaitForCompletion(false)
            .execute();
        awaitPrimaryInSnapshotRestore(indexName);

        // Take a partial=true snapshot. The restoring shard should immediately be recorded as MISSING.
        final SnapshotInfo snapshotInfo = startFullSnapshot(repoName, "partial-snapshot", true).get().getSnapshotInfo();

        assertThat(snapshotInfo.state(), is(SnapshotState.PARTIAL));
        assertThat(snapshotInfo.shardFailures(), hasSize(1));
        assertThat(snapshotInfo.shardFailures().get(0).reason(), containsString(SnapshotsService.SHARD_BEING_RESTORED_REASON));

        unblockAllDataNodes(repoName);
        awaitNoMoreRunningOperations();
    }

    /**
     * A {@code partial=false} snapshot taken while a shard is being restored waits for the restore
     * to complete and then captures the shard, completing as {@link SnapshotState#SUCCESS}.
     * This is the key regression guard: the existing waiting behaviour must not be broken.
     */
    public void testNonPartialSnapshotWhileRestoringWaitsAndSucceeds() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final String indexName = "test-index";
        final String repoName = "test-repo";
        final String sourceSnapshot = "source-snapshot";
        createIndexWithContent(indexName);
        createRepository(repoName, "mock");
        createFullSnapshot(repoName, sourceSnapshot);

        // Delete rather than close: a closed index retains shard data on disk, so the data node skips
        // repo reads during recovery. Deleting forces chunk-blob reads from the repository, which means
        // blockAllDataNodes actually holds the primary in INITIALIZING.
        assertAcked(indicesAdmin().prepareDelete(indexName));

        blockAllDataNodes(repoName);
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot)
            .setIndices(indexName)
            .setWaitForCompletion(false)
            .execute();
        awaitPrimaryInSnapshotRestore(indexName);

        // Start a partial=false snapshot — it must not complete while the restore is blocked.
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "non-partial-snapshot", false);
        awaitNumberOfSnapshotsInProgress(1);
        assertFalse("snapshot must not complete while restore is still in progress", snapshotFuture.isDone());

        // Let the restore finish — the snapshot should now capture the shard and complete successfully.
        unblockAllDataNodes(repoName);
        final SnapshotInfo info = snapshotFuture.get().getSnapshotInfo();

        assertThat(info.state(), is(SnapshotState.SUCCESS));
        assertThat(info.shardFailures(), empty());
    }

    /**
     * In a multi-index snapshot, only the shards whose primaries are being restored become
     * {@link SnapshotsInProgress.ShardState#MISSING}. Healthy shards from other indices are captured
     * in full, confirming that the predicate does not mark the entire snapshot as failed when only
     * a subset of shards are restoring.
     */
    public void testPartialSnapshotMultiIndexOnlyRestoringShardsAreMissing() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final String restoringIndex = "restoring-index";
        final String healthyIndex = "healthy-index";
        final String repoName = "test-repo";
        final String sourceSnapshot = "source-snapshot";
        final String snapshotName = "partial-snapshot";

        createIndexWithContent(restoringIndex);
        createIndexWithContent(healthyIndex);
        createRepository(repoName, "mock");
        createFullSnapshot(repoName, sourceSnapshot);

        // Delete rather than close: a closed index retains shard data on disk, so the data node skips
        // repo reads during recovery. Deleting forces chunk-blob reads from the repository, which means
        // blockAllDataNodes actually holds the primary in INITIALIZING.
        assertAcked(indicesAdmin().prepareDelete(restoringIndex));

        blockAllDataNodes(repoName);
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot)
            .setIndices(restoringIndex)
            .setWaitForCompletion(false)
            .execute();
        awaitPrimaryInSnapshotRestore(restoringIndex);

        // Start snapshot asynchronously — the restoring shard becomes MISSING immediately (master-side),
        // but the healthy shard needs the data node to proceed, which is still blocked.
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, snapshotName, true);

        // Wait until the master has recorded the restoring shard as MISSING in the snapshot entry.
        awaitSnapshotShardMissing(repoName, snapshotName);

        // Unblock data nodes: the healthy shard can now be snapshotted and the snapshot completes.
        // The restoring shard is already MISSING (terminal) and won't be re-evaluated even if the
        // restore later completes.
        unblockAllDataNodes(repoName);
        final SnapshotInfo info = snapshotFuture.get().getSnapshotInfo();

        assertThat(info.state(), is(SnapshotState.PARTIAL));
        assertThat(info.shardFailures(), hasSize(1));
        assertThat(info.shardFailures().get(0).index(), equalTo(restoringIndex));
        assertThat(info.shardFailures().get(0).reason(), containsString(SnapshotsService.SHARD_BEING_RESTORED_REASON));
        assertThat(info.successfulShards(), equalTo(1));

        awaitNoMoreRunningOperations();
    }

    /**
     * After a restore completes and the primary is {@code STARTED}, a subsequent {@code partial=true}
     * snapshot captures the shard normally and completes as {@link SnapshotState#SUCCESS}. This
     * confirms that {@link SnapshotsInProgress.ShardState#MISSING} is assigned only to shards that
     * are actively being restored at snapshot-creation time; a shard that has already finished
     * restoring is captured normally.
     */
    public void testSnapshotAfterRestoreCompletesSucceeds() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final String indexName = "test-index";
        final String repoName = "test-repo";
        final String sourceSnapshot = "source-snapshot";
        createIndexWithContent(indexName);
        createRepository(repoName, "mock");
        createFullSnapshot(repoName, sourceSnapshot);

        assertAcked(indicesAdmin().prepareDelete(indexName));
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute()
            .get();
        ensureGreen(indexName);

        final SnapshotInfo info = startFullSnapshot(repoName, "post-restore-snapshot", true).get().getSnapshotInfo();

        assertThat(info.state(), is(SnapshotState.SUCCESS));
        assertThat(info.shardFailures(), empty());
    }

    /**
     * When a shard snapshot is represented by {@link SnapshotsInProgress.ShardSnapshotStatus#UNASSIGNED_QUEUED}
     * (blocked by a running clone on the same {@link org.elasticsearch.repositories.RepositoryShardId}), and the
     * shard's primary transitions to {@code INITIALIZING} via a restore before the clone completes, the
     * shard is reassigned to {@link SnapshotsInProgress.ShardState#MISSING} when the clone unblocks —
     * not to {@link SnapshotsInProgress.ShardState#WAITING}. This exercises the {@code startShardSnapshot}
     * re-evaluation path rather than the initial assignment path.
     * <p>
     * The restore is started before the clone and snapshot so that the primary is already
     * {@code INITIALIZING} when the clone finishes and {@code startShardSnapshot} fires. The index is
     * deleted (not closed) so that {@code blockAllDataNodes} genuinely holds the restore
     * {@code INITIALIZING} — a closed index retains shard data on disk and skips repo reads, which
     * would let the restore complete before the MISSING state can be observed. The partial snapshot
     * is started after the clone, so its shard is {@link SnapshotsInProgress.ShardSnapshotStatus#UNASSIGNED_QUEUED}
     * behind the clone's {@code INIT} shard — and the primary is already restoring at the time of registration.
     */
    public void testQueuedPartialSnapshotRestoringShardBecomesMissing() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();

        final String indexName = "test-index";
        final String repoName = "test-repo";
        final String sourceSnapshot = "source-snapshot";
        final String snapshotName = "partial-snapshot";
        createIndexWithContent(indexName);
        createRepository(repoName, "mock");
        createFullSnapshot(repoName, sourceSnapshot);

        // Delete rather than close: a closed index retains shard data on disk, so the data node skips
        // repo reads during recovery. Deleting forces chunk-blob reads from the repository, which means
        // blockAllDataNodes actually holds the primary in INITIALIZING.
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Block data nodes so the restore stays INITIALIZING throughout the test.
        blockAllDataNodes(repoName);
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot)
            .setIndices(indexName)
            .setWaitForCompletion(false)
            .execute();
        awaitPrimaryInSnapshotRestore(indexName);

        // Block the master on writing shard-level snapshot metadata so the clone shard stays in INIT.
        // Clones do not copy chunk blobs (they share references), so only metadata writes are blocking points.
        blockMasterOnShardClone(repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = clusterAdmin().prepareCloneSnapshot(
            TEST_REQUEST_TIMEOUT,
            repoName,
            sourceSnapshot,
            "clone-snapshot"
        ).setIndices(indexName).execute();
        waitForBlock(masterName, repoName);

        // Start a partial=true snapshot. Its shard is UNASSIGNED_QUEUED behind the clone's INIT,
        // and the primary is already INITIALIZING (restore in progress).
        final ActionFuture<CreateSnapshotResponse> partialFuture = startFullSnapshot(repoName, snapshotName, true);
        awaitSnapshotShardQueued(repoName, snapshotName);

        // Unblock the master. The clone shard completes, triggering startShardSnapshot for the
        // partial snapshot. initialState has primary INITIALIZING + RestoreInProgress, so the shard
        // is assigned MISSING rather than WAITING.
        unblockNode(repoName, masterName);

        final SnapshotInfo partialInfo = partialFuture.get().getSnapshotInfo();
        assertThat(partialInfo.state(), is(SnapshotState.PARTIAL));
        assertThat(partialInfo.shardFailures(), hasSize(1));
        assertThat(partialInfo.shardFailures().get(0).reason(), containsString(SnapshotsService.SHARD_BEING_RESTORED_REASON));

        assertAcked(cloneFuture.get());
        unblockAllDataNodes(repoName);
        awaitNoMoreRunningOperations();
    }

    /**
     * The {@code partial=false} variant of {@link #testQueuedPartialSnapshotRestoringShardBecomesMissing}:
     * when the queued snapshot has {@code partial=false}, the shard is reassigned to
     * {@link SnapshotsInProgress.ShardState#WAITING} (not {@code MISSING}) and the snapshot completes
     * as {@link SnapshotState#SUCCESS} once the restore finishes.
     * <p>
     * The restore is started before the clone and snapshot so that the primary is already
     * {@code INITIALIZING} when the clone finishes and {@code startShardSnapshot} fires. The index is
     * deleted (not closed) so that {@code blockAllDataNodes} genuinely holds the restore
     * {@code INITIALIZING} — a closed index retains shard data on disk and skips repo reads, which
     * would let the restore complete before the WAITING state can be observed. The non-partial snapshot
     * is started after the clone, so its shard is {@link SnapshotsInProgress.ShardSnapshotStatus#UNASSIGNED_QUEUED}
     * behind the clone's {@code INIT} shard — and the primary is already restoring at the time of registration.
     */
    public void testQueuedNonPartialSnapshotRestoringShardStaysWaiting() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();

        final String indexName = "test-index";
        final String repoName = "test-repo";
        final String sourceSnapshot = "source-snapshot";
        final String snapshotName = "non-partial-snapshot";
        createIndexWithContent(indexName);
        createRepository(repoName, "mock");
        createFullSnapshot(repoName, sourceSnapshot);

        // Delete rather than close: a closed index retains shard data on disk, so the data node skips
        // repo reads during recovery. Deleting forces chunk-blob reads from the repository, which means
        // blockAllDataNodes actually holds the primary in INITIALIZING.
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Block data nodes so the restore stays INITIALIZING throughout the test.
        blockAllDataNodes(repoName);
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot)
            .setIndices(indexName)
            .setWaitForCompletion(false)
            .execute();
        awaitPrimaryInSnapshotRestore(indexName);

        // Start a clone (blocked). The clone's shard (same RepositoryShardId as test-index) goes INIT.
        blockMasterOnShardClone(repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = clusterAdmin().prepareCloneSnapshot(
            TEST_REQUEST_TIMEOUT,
            repoName,
            sourceSnapshot,
            "clone-snapshot"
        ).setIndices(indexName).execute();
        waitForBlock(masterName, repoName);

        // Start a non-partial snapshot. The shard is UNASSIGNED_QUEUED behind the clone's INIT shard,
        // and the primary is already INITIALIZING (restore in progress).
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, snapshotName, false);
        awaitSnapshotShardQueued(repoName, snapshotName);

        // Unblock the master. The clone shard completes, triggering startShardSnapshot for the
        // non-partial snapshot. initialState has primary INITIALIZING + RestoreInProgress, but
        // partial=false → WAITING (not MISSING).
        unblockNode(repoName, masterName);

        // Confirm the shard is WAITING before the restore has a chance to complete — data nodes are
        // still blocked, so this window is stable. This is the key assertion: it proves the
        // re-evaluation path produced WAITING, not just that the snapshot eventually succeeded.
        awaitClusterState(
            state -> SnapshotsInProgress.get(state)
                .forRepo(repoName)
                .stream()
                .anyMatch(
                    e -> e.snapshot().getSnapshotId().getName().equals(snapshotName)
                        && e.shards().values().stream().anyMatch(s -> s.state() == SnapshotsInProgress.ShardState.WAITING)
                )
        );

        // Unblock data nodes so the restore and snapshot can finish.
        unblockAllDataNodes(repoName);

        assertAcked(cloneFuture.get());
        final SnapshotInfo info = snapshotFuture.get().getSnapshotInfo();
        assertThat(info.state(), is(SnapshotState.SUCCESS));
        assertThat(info.shardFailures(), empty());

        awaitNoMoreRunningOperations();
    }

    /**
     * Regression guard for the delete-triggered re-evaluation wiring: when a snapshot delete completes,
     * {@link SnapshotsServiceUtils#shards} is called for any snapshot entry that had shards in
     * {@link SnapshotsInProgress.ShardSnapshotStatus#UNASSIGNED_QUEUED} (because {@code readyToExecute=false}
     * while the delete was running). This test confirms that {@code partial} is correctly threaded through
     * that call so that a restoring {@code INITIALIZING} primary produces
     * {@link SnapshotsInProgress.ShardState#MISSING} (not {@link SnapshotsInProgress.ShardState#WAITING})
     * and the snapshot completes as {@link SnapshotState#PARTIAL}.
     * <p>
     * The predicate behaviour itself is covered by the clone-unblocking tests; this test is specifically
     * about the delete-completion code path passing {@code partial=true} to {@link SnapshotsServiceUtils#shards}.
     */
    public void testDeleteUnblockingPartialSnapshotRestoringShardBecomesMissing() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();

        final String indexName = "test-index";
        final String repoName = "test-repo";
        final String sourceSnapshot = "source-snapshot";
        final String snapshotToDelete = "snapshot-to-delete";
        final String snapshotName = "partial-snapshot";
        createIndexWithContent(indexName);
        createRepository(repoName, "mock");
        createFullSnapshot(repoName, sourceSnapshot);
        createFullSnapshot(repoName, snapshotToDelete);

        // Delete rather than close: closing keeps shard data on disk, which allows ES to skip repo reads
        // during recovery (the existing files match the snapshot). Deleting forces the data node to read
        // chunk blobs from the repository, so blockAllDataNodes actually holds the restore INITIALIZING.
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Block data nodes so the restore stays INITIALIZING throughout.
        blockAllDataNodes(repoName);
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot)
            .setIndices(indexName)
            .setWaitForCompletion(false)
            .execute();
        awaitPrimaryInSnapshotRestore(indexName);

        // Block master on writing the repository index file so the delete is held in SnapshotDeletionsInProgress.
        blockMasterOnWriteIndexFile(repoName);
        clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotToDelete).execute();
        waitForBlock(masterName, repoName);

        // Start a partial=true snapshot while the delete is in flight. readyToExecute=false because
        // SnapshotDeletionsInProgress is non-empty, so the shard is UNASSIGNED_QUEUED.
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, snapshotName, true);
        awaitSnapshotShardQueued(repoName, snapshotName);

        // Unblock the master. The delete completes, removing the SnapshotDeletionsInProgress entry.
        // SnapshotsServiceUtils#shards() re-evaluates the QUEUED shard. Primary is INITIALIZING + restore
        // active + partial=true → MISSING. Snapshot finalizes as PARTIAL.
        unblockNode(repoName, masterName);

        final SnapshotInfo info = snapshotFuture.get().getSnapshotInfo();
        assertThat(info.state(), is(SnapshotState.PARTIAL));
        assertThat(info.shardFailures(), hasSize(1));
        assertThat(info.shardFailures().get(0).reason(), containsString(SnapshotsService.SHARD_BEING_RESTORED_REASON));

        unblockAllDataNodes(repoName);
        awaitNoMoreRunningOperations();
    }

    /**
     * Master failover while a {@code partial=true} snapshot is in-flight and a shard is being restored.
     * The restoring shard is assigned {@link SnapshotsInProgress.ShardState#MISSING} at snapshot-creation
     * time (master-side, before any data-node I/O). The master is then blocked from writing the
     * repository index file so the snapshot stays in {@link SnapshotsInProgress} during the restart.
     * <p>
     * After failover the new master inherits both {@link SnapshotsInProgress} (with the MISSING shard
     * already recorded) and {@link org.elasticsearch.cluster.RestoreInProgress} from cluster state,
     * and must finalize the snapshot as {@link SnapshotState#PARTIAL}.
     */
    public void testPartialSnapshotWithMasterFailoverWhileRestoring() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final String indexName = "test-index";
        final String repoName = "test-repo";
        final String sourceSnapshot = "source-snapshot";
        final String snapshotName = "partial-snapshot";
        createIndexWithContent(indexName);
        createRepository(repoName, "mock");
        createFullSnapshot(repoName, sourceSnapshot);

        // Delete rather than close: a closed index retains shard data on disk, so the data node skips
        // repo reads during recovery. Deleting forces chunk-blob reads from the repository, which means
        // blockAllDataNodes actually holds the primary in INITIALIZING.
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Hold the restore in INITIALIZING by blocking the data node's repo I/O.
        blockAllDataNodes(repoName);
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, sourceSnapshot)
            .setIndices(indexName)
            .setWaitForCompletion(false)
            .execute();
        awaitPrimaryInSnapshotRestore(indexName);

        // Block the master from writing the repository index file so the snapshot stays in
        // SnapshotsInProgress (with the MISSING shard already assigned) during the failover.
        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);

        final String initialMaster = internalCluster().getMasterName();
        startFullSnapshot(repoName, snapshotName, true);
        waitForBlock(initialMaster, repoName);

        // Confirm the original master already recorded the shard as MISSING in cluster state before
        // we trigger failover. This distinguishes "MISSING was assigned before failover" from "the
        // new master re-derived it", which is the invariant the test exists to verify.
        awaitSnapshotShardMissing(repoName, snapshotName);

        // Restart the master. The block is released; the new master picks up SnapshotsInProgress
        // and RestoreInProgress from cluster state and must finalize the snapshot as PARTIAL.
        internalCluster().restartNode(initialMaster);
        ensureStableCluster(3);

        unblockAllDataNodes(repoName);
        awaitNoMoreRunningOperations();

        final SnapshotInfo info = getSnapshot(repoName, snapshotName);
        assertThat(info.state(), is(SnapshotState.PARTIAL));
        assertThat(info.shardFailures(), hasSize(1));
        assertThat(info.shardFailures().get(0).reason(), containsString(SnapshotsService.SHARD_BEING_RESTORED_REASON));
    }

    /**
     * Waits until the named snapshot has at least one shard in
     * {@link SnapshotsInProgress.ShardSnapshotStatus#UNASSIGNED_QUEUED}, confirming the shard is
     * queued behind another in-progress operation on the same repository shard.
     */
    private static void awaitSnapshotShardQueued(String repoName, String snapshotName) {
        awaitClusterState(state -> SnapshotsInProgress.get(state).forRepo(repoName).stream().anyMatch(entry -> {
            if (entry.snapshot().getSnapshotId().getName().equals(snapshotName) == false) {
                return false;
            }
            return entry.shardSnapshotStatusByRepoShardId()
                .values()
                .stream()
                .anyMatch(s -> s == SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED);
        }));
    }

    /**
     * Waits until the named snapshot has at least one shard in
     * {@link SnapshotsInProgress.ShardState#MISSING}, confirming the master has already recorded
     * the restoring shard as failed.
     */
    private static void awaitSnapshotShardMissing(String repoName, String snapshotName) {
        awaitClusterState(
            state -> SnapshotsInProgress.get(state)
                .forRepo(repoName)
                .stream()
                .anyMatch(
                    e -> e.snapshot().getSnapshotId().getName().equals(snapshotName)
                        && e.shards().values().stream().anyMatch(s -> s.state() == SnapshotsInProgress.ShardState.MISSING)
                )
        );
    }

    /** Blocks the master before it writes the shard-level snapshot metadata during a clone. */
    private static void blockMasterOnShardClone(String repoName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnMaster(repoName).setBlockOnWriteShardLevelMeta();
    }

    /**
     * Waits until the primary shard of the given index is {@link ShardRoutingState#INITIALIZING}
     * with a {@link RecoverySource.Type#SNAPSHOT} recovery source, confirming that a restore is
     * actively in progress. At this point {@code RestoreInProgress} is guaranteed to be present in
     * the cluster state (both are set in the same cluster state update by {@code RestoreService}).
     */
    private static void awaitPrimaryInSnapshotRestore(String indexName) {
        awaitClusterState(state -> {
            final IndexRoutingTable indexRouting = state.routingTable().index(indexName);
            if (indexRouting == null) {
                return false;
            }
            final ShardRouting primary = indexRouting.shard(0).primaryShard();
            return primary != null
                && primary.state() == ShardRoutingState.INITIALIZING
                && primary.recoverySource().getType() == RecoverySource.Type.SNAPSHOT;
        });
    }
}
