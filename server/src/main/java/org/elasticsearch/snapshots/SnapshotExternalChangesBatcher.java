/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.RepositoryShardId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.SnapshotsInProgress.completed;

/**
 * Manages batching of cluster state tasks that update in-progress snapshots in response to external cluster
 * changes (e.g. master fail-over, node removal, or shards that were waiting for snapshot becoming available).
 * Ensures at most one cluster state update task is queued at any time, accumulating pending changes between executions.
 */
final class SnapshotExternalChangesBatcher {

    private static final Logger logger = LogManager.getLogger(SnapshotExternalChangesBatcher.class);

    @FunctionalInterface
    interface SnapshotFinalizer {
        /** Writes the completed snapshot to the repository. */
        void finalizeSnapshot(SnapshotsInProgress.Entry entry, Metadata metadata);
    }

    @FunctionalInterface
    interface ClonesStarter {
        /** Dispatches any shard clone operations in {@code INIT} state across all repositories. */
        void startClones(SnapshotsInProgress snapshotsInProgress);
    }

    @FunctionalInterface
    interface DeletionStarter {
        /** Deletes snapshots from the repository. */
        void startDeletion(SnapshotDeletionsInProgress.Entry deleteEntry, IndexVersion minNodeVersion);
    }

    private enum State {
        /* No pending changes & no queued or executing task */
        IDLE,
        /* No pending changes but a task may still be queued or executing */
        NO_CHANGES,
        /* Pending changes. No node change. */
        SHARD_ONLY_CHANGES,
        /* Pending changes with node changes. */
        NODE_CHANGES;

        boolean hasPendingChanges() {
            return this == SHARD_ONLY_CHANGES || this == NODE_CHANGES;
        }
    }

    private final MasterServiceTaskQueue<Task> taskQueue;
    private final Predicate<Snapshot> isInitializingClone;
    private final SnapshotFinalizer snapshotFinalizer;
    private final ClonesStarter clonesStarter;
    private final DeletionStarter deletionStarter;
    private State state = State.IDLE;

    SnapshotExternalChangesBatcher(
        ClusterService clusterService,
        Predicate<Snapshot> isInitializingClone,
        SnapshotFinalizer snapshotFinalizer,
        ClonesStarter clonesStarter,
        DeletionStarter deletionStarter
    ) {
        this.isInitializingClone = isInitializingClone;
        this.snapshotFinalizer = snapshotFinalizer;
        this.clonesStarter = clonesStarter;
        this.deletionStarter = deletionStarter;
        this.taskQueue = clusterService.createTaskQueue("snapshots-service-external-changes", Priority.NORMAL, new Executor());
    }

    SnapshotExternalChangesBatcher(
        MasterServiceTaskQueue<Task> taskQueue,
        Predicate<Snapshot> isInitializingClone,
        SnapshotFinalizer snapshotFinalizer,
        ClonesStarter clonesStarter,
        DeletionStarter deletionStarter
    ) {
        this.taskQueue = taskQueue;
        this.isInitializingClone = isInitializingClone;
        this.snapshotFinalizer = snapshotFinalizer;
        this.clonesStarter = clonesStarter;
        this.deletionStarter = deletionStarter;
    }

    /**
     * Record external cluster changes and submits a cluster state update task if one is not already pending.
     */
    void processExternalChanges(boolean changedNodes, boolean changedShards) {
        if (changedNodes == false && changedShards == false) {
            // Tested: testProcessExternalChangesWithNoChanges
            return;
        } else {
            // Tested: testExternalChangesSubmissionAndExecution (+ others)
        }
        final boolean enqueueTask;
        synchronized (this) {
            enqueueTask = state == State.IDLE;
            if (changedNodes || state == State.NODE_CHANGES) {
                // Tested: testExternalChangesSubmissionAndExecution (+ others)
                state = State.NODE_CHANGES;
            } else {
                // Tested: testExternalChangesTaskResubmissionOnCompletion (+ others)
                state = State.SHARD_ONLY_CHANGES;
            }
        }
        if (enqueueTask) {
            // Tested: all tests on first processExternalChanges call when state==IDLE
            taskQueue.submitTask("update snapshot after external changes", new Task(), null);
        } else {
            // Tested: testExternalChangesSubmissionAndExecution (+ others)
            logger.debug("not enqueueing task, one already pending");
        }
    }

    /**
     * @return true if node changes are pending (both {@code STARTED} and {@code ABORTED} snapshots need updating)
     *         false if only shard-routing changes are pending (only need to update {@code STARTED} snapshots)
     */
    synchronized boolean acquireChangesToExecute() {
        assert state.hasPendingChanges() : "unexpected changes acquired " + state;
        final boolean nodeChanges = state == State.NODE_CHANGES;
        state = State.NO_CHANGES;
        return nodeChanges;
    }

    /**
     * Called after a completed execution of a task. Transitions state back to
     * {@link State#IDLE} if no new changes arrived during execution, re-enqueues a new task otherwise.
     */
    void onTaskCompletion() {
        synchronized (this) {
            if (state == State.NO_CHANGES) {
                // Tested: testExternalChangesSubmissionAndExecution (+ others)
                state = State.IDLE;
                return;
            } else {
                // Tested: testExternalChangesTaskResubmissionOnCompletion
                logger.debug("pending changes remain after task completion, will resubmit");
            }
            assert state.hasPendingChanges() : "unexpected state found on task completion: " + state;
        }
        taskQueue.submitTask("update snapshot after external changes", new Task(), null);
    }

    private void onTaskFailure(Exception e) {
        if (e instanceof NotMasterException == false && e instanceof FailedToCommitClusterStateException == false) {
            // Guarded by assert false
            assert false;
            logger.error("Failed to update snapshot state after shards or node configuration changed", e);
        } else {
            // Tested: testExternalChangesTaskResubmissionOnCompletion
            logger.debug("expected master-failover exception [{}], delegating to onTaskCompletion", e.getMessage());
        }
        onTaskCompletion();
    }

    /**
     * Task submitted to the master service queue. There should always be at most such task in the master queue.
     */
    final class Task implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            onTaskFailure(e);
        }
    }

    final class Executor implements ClusterStateTaskExecutor<Task> {
        @Override
        public ClusterState execute(BatchExecutionContext<Task> batchExecutionContext) {
            final int numberOfTasksInBatch = batchExecutionContext.taskContexts().size();
            assert numberOfTasksInBatch == 1 : "Expected single task in the queue, but was " + numberOfTasksInBatch;

            final TaskContext<Task> taskContext = batchExecutionContext.taskContexts().getFirst();
            final boolean nodesChanged = acquireChangesToExecute();

            final ClusterState currentState = batchExecutionContext.initialState();
            final SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(currentState);
            final SnapshotDeletionsInProgress deletesInProgress = SnapshotDeletionsInProgress.get(currentState);
            final DiscoveryNodes nodes = currentState.nodes();

            final EnumSet<SnapshotsInProgress.State> statesToUpdate;
            if (nodesChanged) {
                // If we are reacting to a change in the cluster node configuration we have to update the shard states of both started
                // and aborted snapshots to potentially fail shards running on the removed nodes
                // Tested: testAbortedSnapshotClusterStateUpdateBasedOnNodeChanges (+ others)
                statesToUpdate = EnumSet.of(SnapshotsInProgress.State.STARTED, SnapshotsInProgress.State.ABORTED);
            } else {
                // We are reacting to shards that started only, which only affects the individual shard states of started snapshots
                // Tested: testStartedWaitingSnapshotClusterStateUpdate (+ others)
                statesToUpdate = EnumSet.of(SnapshotsInProgress.State.STARTED);
            }

            // We keep a cache of shards that failed in this map. If we fail a shardId for a given repository because of
            // a node leaving or shard becoming unassigned for one snapshot, we will also fail it for all subsequent enqueued
            // snapshots for the same repository.
            //
            // TODO: this code duplicates large chunks of the logic in {@link SnapshotsService.SnapshotShardsUpdateContext}.
            // We should refactor it to ideally also go through SnapshotShardsUpdateContext by hand-crafting shard state updates that
            // encapsulate nodes leaving or indices having been deleted and passing them to the executor instead.
            SnapshotsInProgress updatedSnapshots = snapshotsInProgress;

            Collection<SnapshotsInProgress.Entry> finishedSnapshots = new ArrayList<>();
            for (final List<SnapshotsInProgress.Entry> snapshotsInRepo : snapshotsInProgress.entriesByRepo()) {
                boolean changed = false;
                final List<SnapshotsInProgress.Entry> updatedEntriesForRepo = new ArrayList<>();
                final Map<RepositoryShardId, ShardSnapshotStatus> knownFailures = new HashMap<>();
                final var projectId = snapshotsInRepo.get(0).projectId();
                final String repositoryName = snapshotsInRepo.get(0).repository();
                for (SnapshotsInProgress.Entry snapshotEntry : snapshotsInRepo) {
                    if (statesToUpdate.contains(snapshotEntry.state())) {
                        // Tested: testStartedWaitingSnapshotClusterStateUpdate (+ others)
                        if (snapshotEntry.isClone()) {
                            // Tested: testCloneSnapshotWithOrWithoutShardSnapshotStatus (+ others)
                            if (snapshotEntry.shardSnapshotStatusByRepoShardId().isEmpty()) {
                                // Tested: testCloneSnapshotWithOrWithoutShardSnapshotStatus (emptyClone)
                                if (isInitializingClone.test(snapshotEntry.snapshot())) {
                                    // Tested: testCloneSnapshotWithOrWithoutShardSnapshotStatus (isInitializingClone=true)
                                    updatedEntriesForRepo.add(snapshotEntry);
                                } else {
                                    // Tested: testCloneSnapshotWithOrWithoutShardSnapshotStatus (isInitializingClone=false)
                                    logger.debug("removing not yet started clone operation [{}]", snapshotEntry);
                                    changed = true;
                                }
                            } else {
                                // See if any clones may have had a shard become available for execution because of failures
                                // Tested: testCloneSnapshotWithOrWithoutShardSnapshotStatus (+ others)
                                if (deletesInProgress.hasExecutingDeletion(projectId, repositoryName)) {
                                    // Currently executing a delete for this repo, no need to try and update any clone operations.
                                    // The logic for finishing the delete will update running clones with the latest changes.
                                    // Tested: testCloneSnapshotWhenExecutingDeletion
                                    updatedEntriesForRepo.add(snapshotEntry);
                                    continue;
                                } else {
                                    // Tested: testCloneShardsReassignedFromKnownFailures (+ others)
                                    logger.debug("no executing deletion for repo [{}], checking clone shards", repositoryName);
                                }
                                ImmutableOpenMap.Builder<RepositoryShardId, ShardSnapshotStatus> clones = null;
                                InFlightShardSnapshotStates inFlightShardSnapshotStates = null;
                                for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> failureEntry : knownFailures.entrySet()) {
                                    final RepositoryShardId repositoryShardId = failureEntry.getKey();
                                    final ShardSnapshotStatus existingStatus = snapshotEntry.shardSnapshotStatusByRepoShardId()
                                        .get(repositoryShardId);
                                    if (ShardSnapshotStatus.UNASSIGNED_QUEUED.equals(existingStatus)) {
                                        // Tested: testCloneShardsReassignedFromKnownFailures (+ others)
                                        if (inFlightShardSnapshotStates == null) {
                                            // Tested: testCloneShardsReassignedFromKnownFailures (+ others)
                                            inFlightShardSnapshotStates = InFlightShardSnapshotStates.forEntries(updatedEntriesForRepo);
                                        } else {
                                            // Tested: testCloneMultipleShardsReassignedFromKnownFailures
                                            logger.debug("inFlightShardSnapshotStates already initialized");
                                        }
                                        if (inFlightShardSnapshotStates.isActive(
                                            repositoryShardId.indexName(),
                                            repositoryShardId.shardId()
                                        )) {
                                            // we already have this shard assigned to another task
                                            // Tested: testCloneShardSkippedWhenAlreadyActiveInAnotherEntry
                                            continue;
                                        } else {
                                            // Tested: testCloneShardsReassignedFromKnownFailures (+ others)
                                            logger.debug(
                                                "shard [{}] not active in another entry, proceeding with reassignment",
                                                repositoryShardId
                                            );
                                        }
                                        if (clones == null) {
                                            // Tested: testCloneShardsReassignedFromKnownFailures (+ others)
                                            clones = ImmutableOpenMap.builder(snapshotEntry.shardSnapshotStatusByRepoShardId());
                                        } else {
                                            // Tested: testCloneMultipleShardsReassignedFromKnownFailures
                                            logger.debug("clones builder already initialized");
                                        }
                                        // We can use the generation from the shard failure to start the clone operation here
                                        // because #processWaitingShardsAndRemovedNodes adds generations to failure statuses that
                                        // allow us to start another clone.
                                        // The usual route via InFlightShardSnapshotStates is not viable here because it would
                                        // require a consistent view of the RepositoryData which we don't have here because this
                                        // state update runs over all repositories at once.
                                        clones.put(
                                            repositoryShardId,
                                            new ShardSnapshotStatus(nodes.getLocalNodeId(), failureEntry.getValue().generation())
                                        );
                                    } else {
                                        // Tested: testCloneMultipleShardsReassignedFromKnownFailures
                                        logger.debug(
                                            "shard [{}] has status [{}], not UNASSIGNED_QUEUED, skipping",
                                            repositoryShardId,
                                            existingStatus
                                        );
                                    }
                                }
                                if (clones != null) {
                                    // Tested: testCloneShardsReassignedFromKnownFailures (+ others)
                                    changed = true;
                                    updatedEntriesForRepo.add(snapshotEntry.withClones(clones.build()));
                                } else {
                                    // Tested: testCloneSnapshotWithOrWithoutShardSnapshotStatus (+ others)
                                    updatedEntriesForRepo.add(snapshotEntry);
                                }
                            }
                        } else {
                            // Not a clone, and the snapshot is in STARTED or ABORTED state.
                            // Tested: testStartedWaitingSnapshotClusterStateUpdate (+ others)
                            ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards = SnapshotsServiceUtils
                                .processWaitingShardsAndRemovedNodes(
                                    snapshotEntry,
                                    currentState.routingTable(projectId),
                                    nodes,
                                    snapshotsInProgress::isNodeIdForRemoval,
                                    knownFailures
                                );
                            if (shards != null) {
                                // Tested: testAbortedSnapshotClusterStateUpdateBasedOnNodeChanges (+ others)
                                final SnapshotsInProgress.Entry updatedSnapshot = snapshotEntry.withShardStates(shards);
                                changed = true;
                                if (updatedSnapshot.state().completed()) {
                                    // Tested: testAbortedSnapshotClusterStateUpdateBasedOnNodeChanges (+ others)
                                    finishedSnapshots.add(updatedSnapshot);
                                } else {
                                    // Tested: testStartedWaitingSnapshotClusterStateUpdate
                                    logger.debug(
                                        "updated snapshot [{}] not yet completed, stays in state [{}]",
                                        updatedSnapshot.snapshot(),
                                        updatedSnapshot.state()
                                    );
                                }
                                updatedEntriesForRepo.add(updatedSnapshot);
                            } else {
                                // Tested: testNonCloneEntryRetainedUnchangedWhenNoShardChanges
                                updatedEntriesForRepo.add(snapshotEntry);
                            }
                        }
                    } else if (snapshotEntry.repositoryStateId() == org.elasticsearch.repositories.RepositoryData.UNKNOWN_REPO_GEN) {
                        // BwC path, older versions could create entries with unknown repo GEN in INIT or ABORTED state that did not
                        // yet write anything to the repository physically. This means we can simply remove these from the cluster
                        // state without having to do any additional cleanup.
                        // Tested: testBwcUnknownRepoGenEntryCleanup
                        changed = true;
                        logger.debug("[{}] was found in dangling INIT or ABORTED state", snapshotEntry);
                    } else {
                        // Now we're down to completed or un-modified snapshots
                        // Tested: testWaitingDeletionDoesNotSuppressFinalization (+ others)
                        if (snapshotEntry.state().completed() || completed(snapshotEntry.shardSnapshotStatusByRepoShardId().values())) {
                            // Tested: testWaitingDeletionDoesNotSuppressFinalization (+ others)
                            finishedSnapshots.add(snapshotEntry);
                        } else {
                            // Tested: testAbortedSnapshotClusterStateUpdateBasedOnNodeChanges (+ others)
                            logger.debug(
                                "snapshot [{}] in state [{}] not completed, retaining unchanged",
                                snapshotEntry.snapshot(),
                                snapshotEntry.state()
                            );
                        }
                        updatedEntriesForRepo.add(snapshotEntry);
                    }
                }
                if (changed) {
                    // Tested: testStartedWaitingSnapshotClusterStateUpdate (+ others)
                    updatedSnapshots = updatedSnapshots.createCopyWithUpdatedEntriesForRepo(
                        projectId,
                        repositoryName,
                        updatedEntriesForRepo
                    );
                } else {
                    // Tested: testCompletedSnapshotClusterStateUpdate (+ others)
                    logger.debug("No changes at that point");
                }
            }
            final ClusterState res = SnapshotsServiceUtils.readyDeletions(
                updatedSnapshots != snapshotsInProgress
                    ? ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, updatedSnapshots).build()
                    : currentState,
                null
            ).v1();

            taskContext.success(() -> {
                onTaskCompletion();
                clusterStateProcessed(res, finishedSnapshots);
            });
            return res;
        }

        private void clusterStateProcessed(ClusterState newState, Collection<SnapshotsInProgress.Entry> finishedSnapshots) {
            final SnapshotDeletionsInProgress snapshotDeletionsInProgress = SnapshotDeletionsInProgress.get(newState);
            if (finishedSnapshots.isEmpty() == false) {
                // Skip finalization for repos with an active delete, because it will trigger finalization
                // for any completed snapshots when it removes itself from the cluster state.
                // Tested: testCompletedSnapshotClusterStateUpdate (+ others)
                final Set<String> reposWithRunningDeletes = snapshotDeletionsInProgress.getEntries()
                    .stream()
                    .filter(entry -> entry.state() == SnapshotDeletionsInProgress.State.STARTED)
                    .map(SnapshotDeletionsInProgress.Entry::repository)
                    .collect(Collectors.toSet());
                for (SnapshotsInProgress.Entry entry : finishedSnapshots) {
                    if (reposWithRunningDeletes.contains(entry.repository()) == false) {
                        // Tested: testWaitingDeletionDoesNotSuppressFinalization (+ others)
                        snapshotFinalizer.finalizeSnapshot(entry, newState.metadata());
                    } else {
                        // Tested: testCompletedSnapshotClusterStateUpdate
                        logger.debug("skipping finalization for snapshot [{}] in repo with running delete", entry.snapshot());
                    }
                }
                finishedSnapshots.clear();
            } else {
                // Tested: testBwcUnknownRepoGenEntryCleanup (+ others)
                logger.debug("no finished snapshots to process");
            }
            // Tested: all tests (clonesStarter is always called)
            clonesStarter.startClones(SnapshotsInProgress.get(newState));
            // Run newly ready deletes
            for (SnapshotDeletionsInProgress.Entry entry : snapshotDeletionsInProgress.getEntries()) {
                if (entry.state() == SnapshotDeletionsInProgress.State.STARTED) {
                    // Tested: testCloneSnapshotWhenExecutingDeletion
                    deletionStarter.startDeletion(entry, newState.nodes().getMaxDataNodeCompatibleIndexVersion());
                } else {
                    // Tested: testWaitingDeletionDoesNotSuppressFinalization
                    logger.debug("skipping deletion [{}] not in STARTED state", entry);
                }
            }
        }
    }
}
