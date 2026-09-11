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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.MockLog;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests the node-side service transition that an in-place restore over an already-open index requires: the destination index keeps its
 * index UUID and stays {@link IndexMetadata.State#OPEN} but receives a new history UUID, so every data node holding a copy must remove and
 * recreate its index service with reopened-index semantics rather than update it in place, while keeping the shard store on disk so that
 * the restore file diff can reuse identical local Lucene files.
 * <p>
 * {@link #initializeRestoreOverOpenIndex} drives this through the public {@link RestoreService#restoreSnapshot} API, opting in via
 * {@link RestoreSnapshotRequest#restoreOverExisting()}, except for {@link #testOverlappingRestoreTransitionsDoNotCorruptTheSecondRestore},
 * which instead publishes the equivalent transition directly via {@link #initializeRestoreOverOpenIndexBypassingMasterGuard}: that test
 * simulates a hypothetical caller that isn't protected against overlapping restores the way {@link RestoreService#restoreSnapshot} is, so
 * the node-side transition's own robustness needs to be verified independently of any single caller's protection.
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

        // Without the REOPENED transition, applying the restored metadata in place would throw IllegalArgumentException (IndexSettings
        // rejects an in-place history UUID change), which IndicesClusterStateService#updateIndices falls back to handling as an ordinary
        // failed shard: it still ends up reusing the on-disk store (IndexRemovalReason.FAILURE keeps it too) once the shard is retried, so
        // that fallback path would pass the assertions below even though it isn't the single clean transition this test means to verify.
        // Assert directly that no shard failure occurred, so a regression that disables the transition is caught here rather than silently
        // masked.
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

        // the index service was recreated as REOPENED rather than DELETED, so the shard store survived and the restore diff reused it.
        // Every shard's recovery must have downloaded nothing, and across all shards at least some files must have been reused.
        long totalReusedFiles = 0;
        for (RecoveryState recovery : snapshotRecoveryStates()) {
            assertThat("no file should have needed downloading again", recovery.getIndex().recoveredFileCount(), equalTo(0));
            totalReusedFiles += recovery.getIndex().reusedFileCount();
        }
        assertThat("restore should have reused the preserved local Lucene files", totalReusedFiles, greaterThan(0L));
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

        try {
            // fail every repository read on the data node so the restore recovery of the recreated index service cannot succeed, while
            // leaving the master able to resolve the snapshot and publish the restore transition
            setDataNodeControlIOExceptionRate(1.0);
            initializeRestoreOverOpenIndex();

            // the transition is published regardless of the recovery outcome, and the shard exhausts its allocation retries
            assertThat("the transition is published before any recovery is attempted", historyUuid(), notNullValue());
            assertBusy(
                () -> assertThat(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, INDEX_NAME).get().getUnassignedShards(), greaterThan(0))
            );
        } finally {
            setDataNodeControlIOExceptionRate(0.0);
        }

        // the failed attempt must not have discarded the local store, so retrying restores without downloading anything again
        ClusterRerouteUtils.rerouteRetryFailed(client());
        awaitRestoreCompleted();

        assertHitCount(prepareSearch(INDEX_NAME).setSize(0), docCount);
        final long totalReusedFiles = snapshotRecoveryStates().stream().mapToLong(r -> r.getIndex().reusedFileCount()).sum();
        assertThat("retrying must reuse the preserved local store rather than download again", totalReusedFiles, greaterThan(0L));
    }

    public void testRestoreOverAlreadyRestoredIndexAssignsNewHistoryUuidEachTime() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(randomIntBetween(1, 2));

        final int docCount = createRepositoryAndSnapshottedIndex();

        initializeRestoreOverOpenIndex();
        awaitRestoreCompleted();
        final String historyUuidAfterFirstRestore = historyUuid();
        assertThat("the first restore must assign a history UUID", historyUuidAfterFirstRestore, notNullValue());

        // restore again over the index that the previous restore left open, exercising the same open-to-open transition a second time
        initializeRestoreOverOpenIndex();
        awaitRestoreCompleted();

        assertThat(
            "a second restore over an already-restored index must assign a fresh history UUID rather than reusing the previous one",
            historyUuid(),
            not(equalTo(historyUuidAfterFirstRestore))
        );
        assertHitCount(prepareSearch(INDEX_NAME).setSize(0), docCount);
    }

    public void testRestoreOverOpenIndexWithReplicaRecreatesBothCopies() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        final int docCount = createRepositoryAndSnapshottedIndex(1);

        // the positive "removing index (REOPENED)" signal is only logged at DEBUG, so raise the level for the duration of the capture
        final Logger indicesClusterStateServiceLogger = LogManager.getLogger(IndicesClusterStateService.class);
        final Level originalLevel = indicesClusterStateServiceLogger.getLevel();
        Loggers.setLevel(indicesClusterStateServiceLogger, Level.DEBUG);
        try (var mockLog = MockLog.capture(IndicesClusterStateService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no shard failure on either the primary or the replica copy while applying the restore transition",
                    IndicesClusterStateService.class.getName(),
                    Level.WARN,
                    "marking and sending shard failed"
                )
            );
            // one "removing index (REOPENED)" per node holding a copy of the index: the primary-holding node and the replica-holding
            // node each independently recreate their own local index service, since IndicesClusterStateService#removeIndicesAndShards
            // detects the history UUID transition per node, before it ever considers whether that node's shard copy is still assigned
            final SeenCountExpectation reopenedRemovalExpectation = new SeenCountExpectation(
                "index removed with reason REOPENED on both the primary and the replica copy",
                IndicesClusterStateService.class.getName(),
                Level.DEBUG,
                "removing index (REOPENED)",
                2
            );
            mockLog.addExpectation(reopenedRemovalExpectation);

            initializeRestoreOverOpenIndex();
            awaitRestoreCompleted();

            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(indicesClusterStateServiceLogger, originalLevel);
        }

        assertThat("restore must assign a history UUID", historyUuid(), notNullValue());
        // force a search onto each copy by node so that a copy which silently failed to restore correctly cannot hide behind the other
        for (ShardRouting copy : shardRoutings()) {
            assertHitCount(prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_nodes:" + copy.currentNodeId()), docCount);
        }
    }

    public void testOverlappingRestoreTransitionsDoNotCorruptTheSecondRestore() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final int docCount = createRepositoryAndSnapshottedIndex();
        final RestoreTarget restoreTarget = resolveRestoreTarget();

        // block the first restore's recovery mid-flight so a second transition can be published before it completes, exactly as an
        // ungated caller of this node-side transition could do today: nothing here prevents it. Block on any repository read, not just
        // data files: this restore's local store is unchanged since the snapshot, so the file diff reuses every data file locally and
        // never reads one from the repository at all; recovery still always reads the snapshot's shard-level metadata first, so blocking
        // there is what actually catches it mid-flight
        blockNodeOnAnyFiles(REPOSITORY_NAME, dataNode);
        try {
            initializeRestoreOverOpenIndexBypassingMasterGuard(restoreTarget);
            waitForBlock(dataNode, REPOSITORY_NAME);

            final String historyUuidAfterFirstTransition = historyUuid();
            assertThat(
                "the first transition must apply before its recovery can block reading data",
                historyUuidAfterFirstTransition,
                notNullValue()
            );
            // The recovery is blocked on the repository, so the first restore cannot have completed: its shard has not STARTED. We do
            // not assert the exact routing state, because a blocked recovery may show up as INITIALIZING or, briefly, as UNASSIGNED
            // between allocation attempts (both are valid mid-flight states); asserting INITIALIZING specifically is racy (see #157793).
            assertThat(
                "the first restore must still be in flight (its shard not yet started) when the second transition publishes",
                primaryShardRouting().state(),
                not(equalTo(ShardRoutingState.STARTED))
            );

            // publish a second transition over the same, still-recovering shard, bypassing RestoreService#restoreSnapshot's own
            // guard against overlapping restores of the same index, to prove the node-side transition itself tolerates this
            initializeRestoreOverOpenIndexBypassingMasterGuard(restoreTarget);

            assertThat(
                "the second transition must assign yet another new history UUID, not resume the first one",
                historyUuid(),
                not(equalTo(historyUuidAfterFirstTransition))
            );
        } finally {
            unblockNode(REPOSITORY_NAME, dataNode);
        }

        ensureGreen(INDEX_NAME);
        assertHitCount(prepareSearch(INDEX_NAME).setSize(0), docCount);

        // The second restore completes cleanly: the node-side transition itself tolerates being invoked again before the first
        // recovery finishes. But the first restore's RestoreInProgress entry is a known, currently-unaddressed casualty of this: its
        // shard's routing was overwritten outright by the second transition rather than transitioned through the ordinary allocation
        // lifecycle (shard failed / shard started) that RestoreService.RestoreInProgressUpdater listens for, so nothing ever reports it
        // finished, and it lingers in cluster state indefinitely. This documents that behavior as it is.
        final long remainingRestoreInProgressEntries = StreamSupport.stream(
            RestoreInProgress.get(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState()).spliterator(),
            false
        ).count();
        assertThat(
            "the superseded first restore's RestoreInProgress entry is left dangling today; if this has been fixed, tighten this "
                + "assertion to require RestoreInProgress to be empty",
            remainingRestoreInProgressEntries,
            equalTo(1L)
        );
    }

    /**
     * The restore over an open index preserves the existing close-index safety rule rather than cancelling a conflicting snapshot: it must
     * reject before publishing anything if an active snapshot already includes the destination index, leaving the index untouched, and a
     * plain retry after that snapshot finishes must then succeed.
     */
    public void testRestoreOverOpenIndexRejectsActiveSnapshotConflict() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        createRepositoryAndSnapshottedIndex();

        // block on any file write, not just data files: a second snapshot of an unchanged index writes no new data blobs (the
        // segments are unchanged and deduplicated), but it always rewrites its shard-level snapshot metadata files
        blockNodeOnAnyFiles(REPOSITORY_NAME, dataNode);
        final ActionFuture<CreateSnapshotResponse> blockingSnapshot = startFullSnapshot(REPOSITORY_NAME, "blocking-snap");
        waitForBlock(dataNode, REPOSITORY_NAME);
        try {
            final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = restoreOverOpenIndexFuture();
            final SnapshotInProgressException e = expectThrows(
                SnapshotInProgressException.class,
                () -> future.actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(e.getMessage(), containsString("being snapshotted"));
        } finally {
            unblockAllDataNodes(REPOSITORY_NAME);
            blockingSnapshot.actionGet(TEST_REQUEST_TIMEOUT);
        }

        assertThat("a rejected restore must leave the destination unchanged", historyUuid(), nullValue());

        // the conflict is transient: a plain retry after the snapshot finishes succeeds
        initializeRestoreOverOpenIndex();
        awaitRestoreCompleted();
        assertThat(historyUuid(), notNullValue());
    }

    /**
     * A restore covering more than one open-index destination is all-or-nothing: a conflict on any one target must leave every target
     * unchanged, not just the conflicting one.
     */
    public void testRestoreOverMultipleOpenIndicesIsAllOrNothing() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String otherIndex = "test-idx-2";
        createRepository(REPOSITORY_NAME, "mock");
        createIndex(INDEX_NAME, indexSettingsNoReplicas(1).build());
        createIndex(otherIndex, indexSettingsNoReplicas(1).build());
        ensureGreen(INDEX_NAME, otherIndex);
        createFullSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME);

        // only "otherIndex" conflicts; INDEX_NAME would pass validation on its own, so leaving it unchanged too demonstrates that the
        // restore is genuinely all-or-nothing rather than skipping just the conflicting target
        blockNodeOnAnyFiles(REPOSITORY_NAME, dataNode);
        final ActionFuture<CreateSnapshotResponse> blockingSnapshot = clusterAdmin().prepareCreateSnapshot(
            TEST_REQUEST_TIMEOUT,
            REPOSITORY_NAME,
            "blocking-snap"
        ).setIndices(otherIndex).setWaitForCompletion(true).execute();
        waitForBlock(dataNode, REPOSITORY_NAME);
        try {
            final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = new PlainActionFuture<>();
            restoreService().restoreSnapshot(
                ProjectId.DEFAULT,
                new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, REPOSITORY_NAME, SNAPSHOT_NAME).indices(INDEX_NAME, otherIndex)
                    .restoreOverExisting(true),
                future
            );
            expectThrows(SnapshotInProgressException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        } finally {
            unblockAllDataNodes(REPOSITORY_NAME);
            blockingSnapshot.actionGet(TEST_REQUEST_TIMEOUT);
        }

        assertThat("the target unrelated to the conflict must still be left unchanged", historyUuid(INDEX_NAME), nullValue());
        assertThat("the conflicting target must be left unchanged", historyUuid(otherIndex), nullValue());
    }

    /**
     * The ordinary public restore API ({@link RestoreService#restoreSnapshot}) can also reach the open-index restore path, but only when
     * the caller explicitly opts in via {@link RestoreSnapshotRequest#restoreOverExisting}; the default behavior (reject an open
     * destination) must be unchanged.
     */
    public void testOrdinaryRestoreCanTargetOpenIndexWhenRequested() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(randomIntBetween(1, 2));

        final int docCount = createRepositoryAndSnapshottedIndex();
        assertThat(historyUuid(), nullValue());

        final PlainActionFuture<RestoreService.RestoreCompletionResponse> rejected = new PlainActionFuture<>();
        restoreService().restoreSnapshot(
            ProjectId.DEFAULT,
            new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, REPOSITORY_NAME, SNAPSHOT_NAME).indices(INDEX_NAME),
            rejected
        );
        final SnapshotRestoreException e = expectThrows(SnapshotRestoreException.class, () -> rejected.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(e.getMessage(), containsString("open index"));
        assertThat("the default (opted-out) behavior must leave the index unchanged", historyUuid(), nullValue());

        final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = new PlainActionFuture<>();
        restoreService().restoreSnapshot(
            ProjectId.DEFAULT,
            new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, REPOSITORY_NAME, SNAPSHOT_NAME).indices(INDEX_NAME).restoreOverExisting(true),
            future
        );
        future.actionGet(TEST_REQUEST_TIMEOUT);
        awaitRestoreCompleted();

        assertThat(historyUuid(), notNullValue());
        assertHitCount(prepareSearch(INDEX_NAME).setSize(0), docCount);
    }

    /**
     * {@link RestoreSnapshotRequest#restoreOverExisting()} is a superset of the ordinary restore-over-closed behavior: a closed destination
     * is never an open-index target (only OPEN destinations are), so it falls through to the ordinary closed-index restore path. Setting
     * the flag over a closed index must therefore still succeed, exactly as an ordinary restore would.
     */
    public void testRestoreOverExistingCanTargetClosedIndex() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(randomIntBetween(1, 2));

        final int docCount = createRepositoryAndSnapshottedIndex();

        assertAcked(indicesAdmin().prepareClose(INDEX_NAME));

        final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = new PlainActionFuture<>();
        restoreService().restoreSnapshot(
            ProjectId.DEFAULT,
            new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, REPOSITORY_NAME, SNAPSHOT_NAME).indices(INDEX_NAME).restoreOverExisting(true),
            future
        );
        future.actionGet(TEST_REQUEST_TIMEOUT);
        awaitRestoreCompleted();

        assertThat("restoring over the reopened index must assign a history UUID", historyUuid(), notNullValue());
        assertHitCount(prepareSearch(INDEX_NAME).setSize(0), docCount);
    }

    /**
     * {@link RestoreSnapshotRequest#restoreOverExisting()} combined with a rename applies to the renamed destination: open-index targets
     * are resolved by renamed name, so if the renamed destination already exists and is open, the restore is applied over it in place
     * rather than rejected.
     */
    public void testRestoreOverExistingWithRenameTargetsRenamedOpenIndex() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final int docCount = createRepositoryAndSnapshottedIndex();

        // a separate, already-open index that the rename resolves to as the destination, seeded with different content so the restore is
        // observably replacing it rather than adopting it
        final String renamedIndex = INDEX_NAME + "-restored";
        // the rename destination must have the same shard count as the snapshotted source, which the shared setup randomizes
        final int numberOfShards = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject(ProjectId.DEFAULT)
            .index(INDEX_NAME)
            .getNumberOfShards();
        createIndex(renamedIndex, indexSettings(numberOfShards, 0).build());
        prepareIndex(renamedIndex).setId("pre").setSource("field", "pre-existing").get();
        indicesAdmin().prepareFlush(renamedIndex).get();
        ensureGreen(renamedIndex);
        assertThat("the rename destination starts without a restore history UUID", historyUuid(renamedIndex), nullValue());

        final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = new PlainActionFuture<>();
        restoreService().restoreSnapshot(
            ProjectId.DEFAULT,
            new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, REPOSITORY_NAME, SNAPSHOT_NAME).indices(INDEX_NAME)
                .renamePattern("(.+)")
                .renameReplacement("$1-restored")
                .restoreOverExisting(true),
            future
        );
        future.actionGet(TEST_REQUEST_TIMEOUT);
        awaitRestoreCompleted(renamedIndex);

        // the open renamed destination was restored over in place: it gains a restore history UUID and holds the snapshot's contents
        // (docCount docs), not the single pre-existing document
        assertThat(historyUuid(renamedIndex), notNullValue());
        assertHitCount(prepareSearch(renamedIndex).setSize(0), docCount);
        // the original source index is untouched by a renamed restore
        assertThat("the un-renamed source index must not be restored over", historyUuid(INDEX_NAME), nullValue());
    }

    /**
     * Restoring over an open index whose primary is currently {@link ShardRoutingState#UNASSIGNED} must still work. The operation replaces
     * the destination's routing outright, so a non-STARTED destination is not a special case. The destination is left unassigned here by
     * stopping the only data node (the index stays open, just red). The restore is published against that unassigned destination, and a
     * fresh data node then lets the restored shards allocate and recover from the snapshot.
     */
    public void testRestoreOverOpenIndexWhosePrimaryIsUnassigned() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final int docCount = createRepositoryAndSnapshottedIndex();

        // stop the only data node so the open destination's primary becomes UNASSIGNED (the index stays open, just red)
        internalCluster().stopNode(dataNode);
        assertBusy(() -> assertThat(primaryShardRouting().state(), equalTo(ShardRoutingState.UNASSIGNED)));

        // publish the restore-over while the destination is unassigned; this master-side update needs no data node
        final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = new PlainActionFuture<>();
        restoreService().restoreSnapshot(
            ProjectId.DEFAULT,
            new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, REPOSITORY_NAME, SNAPSHOT_NAME).indices(INDEX_NAME).restoreOverExisting(true),
            future
        );
        future.actionGet(TEST_REQUEST_TIMEOUT);

        // a fresh data node lets the restoring shards allocate and recover from the snapshot
        internalCluster().startDataOnlyNode();
        awaitRestoreCompleted();

        assertThat("restore over an unassigned destination must still assign a history UUID", historyUuid(), notNullValue());
        assertHitCount(prepareSearch(INDEX_NAME).setSize(0), docCount);
    }

    private int createRepositoryAndSnapshottedIndex() throws Exception {
        // Randomize the replica count within what the running cluster can allocate, so that across CI seeds the restore-over-open operation
        // is exercised against different copy layouts. The operation is expected to be routing-state-agnostic, and randomizing here guards
        // against a regression that made it copy-count sensitive. Tests that need a specific layout start a fixed number of data nodes
        // (a single node bounds this to zero replicas); tests that want replica coverage start more than one.
        return createRepositoryAndSnapshottedIndex(randomIntBetween(0, Math.max(0, internalCluster().numDataNodes() - 1)));
    }

    private int createRepositoryAndSnapshottedIndex(int numberOfReplicas) throws Exception {
        createRepository(REPOSITORY_NAME, "mock");
        // Randomize the shard count as well, so the operation is exercised against different routing-table breadths.
        final int numberOfShards = randomIntBetween(1, 3);
        createIndex(INDEX_NAME, indexSettings(numberOfShards, numberOfReplicas).build());

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
    private record RestoreTarget(Snapshot snapshot, SnapshotInfo snapshotInfo, IndexId indexId, IndexMetadata snapshotIndexMetadata) {}

    private RestoreTarget resolveRestoreTarget() throws IOException {
        final SnapshotInfo snapshotInfo = getSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME);
        final RepositoryData repositoryData = getRepositoryData(REPOSITORY_NAME);
        final IndexId indexId = repositoryData.resolveIndexId(INDEX_NAME);
        final Repository repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(REPOSITORY_NAME);
        return new RestoreTarget(
            new Snapshot(REPOSITORY_NAME, snapshotInfo.snapshotId()),
            snapshotInfo,
            indexId,
            repository.getSnapshotIndexMetaData(repositoryData, snapshotInfo.snapshotId(), indexId)
        );
    }

    /**
     * Drives the transition under test through the real public restore API: an ordinary {@link RestoreService#restoreSnapshot} that opts
     * into overwriting the open destination via {@link RestoreSnapshotRequest#restoreOverExisting()}. The data node holding the shard
     * must apply, as a single change, an index that stays open and keeps its index UUID but gains a new history UUID together with a
     * restoring shard assigned to it.
     */
    private void initializeRestoreOverOpenIndex() {
        final ShardRouting startedPrimary = primaryShardRouting();
        assertThat(startedPrimary.state(), equalTo(ShardRoutingState.STARTED));
        safeGet(restoreOverOpenIndexFuture());
    }

    /**
     * Submits the restore-over-open-index without waiting for it, so that callers expecting a specific failure (via {@link #expectThrows})
     * can observe the real exception type through {@link PlainActionFuture#actionGet} instead of {@link #safeGet}, which converts every
     * failure into a generic {@link AssertionError}.
     */
    private PlainActionFuture<RestoreService.RestoreCompletionResponse> restoreOverOpenIndexFuture() {
        final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = new PlainActionFuture<>();
        restoreService().restoreSnapshot(
            ProjectId.DEFAULT,
            new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, REPOSITORY_NAME, SNAPSHOT_NAME).indices(INDEX_NAME).restoreOverExisting(true),
            future
        );
        return future;
    }

    private RestoreService restoreService() {
        return internalCluster().getCurrentMasterNodeInstance(RestoreService.class);
    }

    /**
     * Publishes, in a single cluster-state update, the same transition the open-index restore path publishes, but without going through
     * {@link RestoreService#restoreSnapshot}'s own guard against overlapping restores of the same index. Retained solely for
     * {@link #testOverlappingRestoreTransitionsDoNotCorruptTheSecondRestore}, which needs to publish two overlapping transitions over the
     * same index to verify that the node-side transition itself is robust to that, independent of any caller's guard against it.
     */
    private void initializeRestoreOverOpenIndexBypassingMasterGuard(RestoreTarget restoreTarget) {
        safeGet(publishRestoreInitialization(restoreTarget));
    }

    /**
     * Publishes, in a single cluster-state update, the transition that the master-side atomic open-index restore operation publishes:
     * the destination index keeps its index UUID and stays open, but receives a new history UUID, snapshot-recovery routing, rebuilt blocks
     * and a correlated {@link RestoreInProgress} entry.
     * <p>
     * The routing is rebuilt via {@link RoutingTable.Builder#addAsRestore}, the same call the real restore code makes: every copy
     * (primary and replicas) is freshly unassigned, and the trailing {@link AllocationService#reroute} call below resolves them before
     * this method's result is ever published, exactly as it does in production. No cluster state with the shards actually unassigned is
     * ever observed by a data node.
     */
    private PlainActionFuture<Void> publishRestoreInitialization(RestoreTarget restoreTarget) {
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

                // mirrors RestoreService#restoreOverExistingIndex: same index UUID, open, but a new history UUID
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
                    restoreTarget.snapshotInfo().version(),
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
                            .addAsRestore(restoredIndexMetadata, recoverySource)
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
        awaitRestoreCompleted(INDEX_NAME);
    }

    private void awaitRestoreCompleted(String indexName) throws Exception {
        assertBusy(
            () -> assertThat(
                RestoreInProgress.get(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState()).isEmpty(),
                equalTo(true)
            )
        );
        ensureGreen(indexName);
    }

    private ShardRouting primaryShardRouting() {
        return shardRoutingTable().primaryShard();
    }

    /**
     * @return every currently assigned copy (primary and replicas) of the index's single shard
     */
    private List<ShardRouting> shardRoutings() {
        return shardRoutingTable().assignedShards();
    }

    private IndexShardRoutingTable shardRoutingTable() {
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .routingTable(ProjectId.DEFAULT)
            .index(INDEX_NAME)
            .shard(0);
    }

    /**
     * @return the index's current history UUID, or {@code null} if it has never been restored over
     */
    @Nullable
    private String historyUuid() {
        return historyUuid(INDEX_NAME);
    }

    @Nullable
    private String historyUuid(String indexName) {
        final ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        return state.metadata().getProject(ProjectId.DEFAULT).index(indexName).getSettings().get(IndexMetadata.SETTING_HISTORY_UUID);
    }

    private static void setDataNodeControlIOExceptionRate(double rate) {
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository) repositoriesService.repository(REPOSITORY_NAME)).setRandomControlIOExceptionRate(rate);
        }
    }

    private List<RecoveryState> snapshotRecoveryStates() {
        final RecoveryResponse response = indicesAdmin().prepareRecoveries(INDEX_NAME).get();
        final List<RecoveryState> states = response.shardRecoveryStates()
            .get(INDEX_NAME)
            .stream()
            .filter(state -> state.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT)
            .toList();
        assertThat("expected at least one snapshot recovery (one per primary shard)", states, not(empty()));
        return states;
    }

    /**
     * Unlike {@link MockLog.SeenEventExpectation}, which is satisfied by a single matching log event, this counts every matching event
     * across all captured nodes and requires at least {@code expectedCount} of them, so that a message logged once per node (such as an
     * index removal reason) can be asserted to have happened on every node expected to log it, not just one of them.
     */
    private static final class SeenCountExpectation implements MockLog.LoggingExpectation {
        private final String name;
        private final String logger;
        private final Level level;
        private final String message;
        private final int expectedCount;
        private final AtomicInteger seenCount = new AtomicInteger();

        SeenCountExpectation(String name, String logger, Level level, String message, int expectedCount) {
            this.name = name;
            this.logger = logger;
            this.level = level;
            this.message = message;
            this.expectedCount = expectedCount;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel().equals(level)
                && event.getLoggerName().equals(logger)
                && event.getMessage().getFormattedMessage().contains(message)) {
                seenCount.incrementAndGet();
            }
        }

        @Override
        public void assertMatched() {
            assertThat(
                "expected to see " + name + " at least " + expectedCount + " time(s)",
                seenCount.get(),
                greaterThanOrEqualTo(expectedCount)
            );
        }
    }

    /**
     * Tests that restoring over an open index fails if the source snapshot is being deleted.
     */
    public void testRestoreOverOpenIndexRejectedWhileSourceSnapshotIsBeingDeleted() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepositoryAndSnapshottedIndex();

        // Resolve the exact restore target up front. Once the deletion starts, it removes the source snapshot's blobs, so resolving it
        // again would fail for the unrelated reason that the snapshot is gone. This test is specifically about the in-progress-deletion
        // guard.
        final SnapshotInfo snapshotInfo = getSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME);
        final Snapshot snapshot = new Snapshot(REPOSITORY_NAME, snapshotInfo.snapshotId());
        final RestoreService.OpenIndexRestoreTarget target = openIndexTarget(INDEX_NAME);

        // Hold the deletion of the source snapshot in progress. Block the master before it finalizes the repository update that would
        // remove the deletion entry, so SnapshotDeletionsInProgress still lists the source snapshot when the restore is submitted.
        blockMasterOnWriteIndexFile(REPOSITORY_NAME);
        final ActionFuture<AcknowledgedResponse> blockedDeletion = startDeleteSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME);
        try {
            waitForBlock(internalCluster().getMasterName(), REPOSITORY_NAME);
            awaitNDeletionsInProgress(1);

            final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = new PlainActionFuture<>();
            restoreService().restoreSnapshotOverOpenIndices(
                ProjectId.DEFAULT,
                snapshot,
                snapshotInfo,
                TEST_REQUEST_TIMEOUT,
                UUIDs.randomBase64UUID(),
                List.of(target),
                future
            );
            final ConcurrentSnapshotExecutionException e = expectThrows(
                ConcurrentSnapshotExecutionException.class,
                () -> future.actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(e.getMessage(), containsString("snapshot deletion is in-progress"));
        } finally {
            unblockNode(REPOSITORY_NAME, internalCluster().getMasterName());
        }
        blockedDeletion.actionGet(TEST_REQUEST_TIMEOUT);

        assertThat("a restore rejected by the in-progress-deletion guard must leave the destination unchanged", historyUuid(), nullValue());
    }

    /**
     * The snapshot-deletion race, restore-wins outcome: once the restore-over has published its {@link RestoreInProgress} entry, a delete
     * of the source snapshot is rejected while that restore is still running, so the snapshot the restore depends on cannot be removed out
     * from under it. Once the restore completes and its entry clears, the snapshot could be deleted normally again.
     */
    public void testDeletingSourceSnapshotIsRejectedWhileRestoreOverOpenIndexIsInFlight() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        createRepositoryAndSnapshottedIndex();

        // Block the restore's recovery mid-flight so its RestoreInProgress entry stays in cluster state while we attempt the delete. Block
        // on
        // any repository read: the local store is unchanged since the snapshot, so the file diff reuses every data file locally, but
        // recovery
        // still reads the snapshot's shard-level metadata first, which is what this catches.
        blockNodeOnAnyFiles(REPOSITORY_NAME, dataNode);
        final PlainActionFuture<RestoreService.RestoreCompletionResponse> restore = restoreOverOpenIndexFuture();
        try {
            waitForBlock(dataNode, REPOSITORY_NAME);
            assertThat("the restore-over transition must be published before its recovery blocks", historyUuid(), notNullValue());

            final ConcurrentSnapshotExecutionException e = expectThrows(
                ConcurrentSnapshotExecutionException.class,
                clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY_NAME, SNAPSHOT_NAME)
            );
            assertThat(e.getMessage(), containsString("cannot delete snapshot during a restore"));
        } finally {
            unblockAllDataNodes(REPOSITORY_NAME);
        }

        // once unblocked the restore completes, and the rejected delete left the source snapshot intact
        safeGet(restore);
        awaitRestoreCompleted();
        assertThat(historyUuid(), notNullValue());
        assertThat(getSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME).snapshotId().getName(), equalTo(SNAPSHOT_NAME));
    }

    /**
     * This tests what happens when we restore over an open index whose destination was deleted, and its name is now absent. Before the
     * cluster-state update is applied we expect it to be rejected rather than silently creating a new index. The caller resolved an exact
     * identity to restore over, and if that index is gone, the precondition no longer holds, so we fail instead of diverging from the
     * caller's intent by creating a fresh index with a different index UUID. A destination deleted and recreated under the same name is
     * rejected by validateExistingOpenIndexForRestore on the index-UUID mismatch. This test covers the deleted-and-not-recreated case,
     * where the name resolves to nothing.
     */
    public void testRestoreOverOpenIndexRejectedWhenDestinationWasDeleted() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepositoryAndSnapshottedIndex();

        // Resolve the exact restore target while the index still exists, then delete it, simulating a destination removed between the
        // caller
        // resolving its identity and this restore being applied.
        final SnapshotInfo snapshotInfo = getSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME);
        final Snapshot snapshot = new Snapshot(REPOSITORY_NAME, snapshotInfo.snapshotId());
        final RestoreService.OpenIndexRestoreTarget target = openIndexTarget(INDEX_NAME);
        assertAcked(indicesAdmin().prepareDelete(INDEX_NAME));

        final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = new PlainActionFuture<>();
        restoreService().restoreSnapshotOverOpenIndices(
            ProjectId.DEFAULT,
            snapshot,
            snapshotInfo,
            TEST_REQUEST_TIMEOUT,
            UUIDs.randomBase64UUID(),
            List.of(target),
            future
        );
        final SnapshotRestoreException e = expectThrows(SnapshotRestoreException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(e.getMessage(), containsString("no longer exists in the cluster state"));

        assertThat(
            "a restore over a deleted destination must be rejected, not silently create a new index",
            internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
                .state()
                .metadata()
                .getProject(ProjectId.DEFAULT)
                .index(INDEX_NAME),
            nullValue()
        );
    }

    /**
     * Resolves the {@link RestoreService.OpenIndexRestoreTarget} for a single open destination index: its exact current identity (name and
     * index UUID) plus the repository-side {@link IndexId} and {@link IndexMetadata} of the snapshot to restore it from.
     */
    private RestoreService.OpenIndexRestoreTarget openIndexTarget(String indexName) throws IOException {
        final SnapshotInfo snapshotInfo = getSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME);
        final RepositoryData repositoryData = getRepositoryData(REPOSITORY_NAME);
        final IndexId indexId = repositoryData.resolveIndexId(indexName);
        final Repository repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(REPOSITORY_NAME);
        final IndexMetadata snapshotIndexMetadata = repository.getSnapshotIndexMetaData(repositoryData, snapshotInfo.snapshotId(), indexId);
        return new RestoreService.OpenIndexRestoreTarget(currentIndex(indexName), indexId, snapshotIndexMetadata);
    }

    private Index currentIndex(String indexName) {
        return internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .state()
            .metadata()
            .getProject(ProjectId.DEFAULT)
            .index(indexName)
            .getIndex();
    }

    /**
     * This tests concurrent restores of the same open index. While one restore-over is in flight, a second one submitted through the master
     * is rejected by {@link RestoreService}'s guard against overlapping restores of the same index, leaving the first restore untouched.
     * (The behavior of non-master nodes encountering overlapping transitions that deliberately bypass this guard is covered separately by
     * {@link #testOverlappingRestoreTransitionsDoNotCorruptTheSecondRestore}.)
     */
    public void testRestoreOverOpenIndexRejectedWhileAnotherRestoreOverIsInFlight() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        createRepositoryAndSnapshottedIndex();

        // block the first restore's recovery so its RestoreInProgress entry and restoring shard persist while the second is submitted
        blockNodeOnAnyFiles(REPOSITORY_NAME, dataNode);
        final PlainActionFuture<RestoreService.RestoreCompletionResponse> first = restoreOverOpenIndexFuture();
        try {
            waitForBlock(dataNode, REPOSITORY_NAME);
            final String historyUuidAfterFirst = historyUuid();
            assertThat("the first restore-over must publish before its recovery blocks", historyUuidAfterFirst, notNullValue());

            final PlainActionFuture<RestoreService.RestoreCompletionResponse> second = restoreOverOpenIndexFuture();
            final SnapshotRestoreException e = expectThrows(SnapshotRestoreException.class, () -> second.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(e.getMessage(), containsString("already being restored"));

            assertThat(
                "the rejected second restore must not disturb the in-flight first restore",
                historyUuid(),
                equalTo(historyUuidAfterFirst)
            );
        } finally {
            unblockAllDataNodes(REPOSITORY_NAME);
        }

        safeGet(first);
        awaitRestoreCompleted();
        assertThat(historyUuid(), notNullValue());
    }
}
