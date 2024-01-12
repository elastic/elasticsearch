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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatelessCommitCleaner extends AbstractLifecycleComponent implements ClusterStateListener {
    private final Logger logger = LogManager.getLogger(StatelessCommitCleaner.class);

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final StatelessClusterConsistencyService consistencyService;
    private final ThreadPool threadPool;
    private final ObjectStoreService objectStoreService;
    private final ConcurrentLinkedQueue<StaleCompoundCommit> pendingCommitsToDelete = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<StaleCompoundCommit> pendingCommitsOfRelocatingPrimariesToDelete = new ConcurrentLinkedQueue<>();
    private final Semaphore consistencyCheckPermit = new Semaphore(1);
    private final Semaphore relocatingPrimariesCheckPermit = new Semaphore(1);

    public StatelessCommitCleaner(
        StatelessClusterConsistencyService consistencyService,
        ThreadPool threadPool,
        ObjectStoreService objectStoreService
    ) {
        this.consistencyService = consistencyService;
        this.threadPool = threadPool;
        this.objectStoreService = objectStoreService;
    }

    void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
        pendingCommitsToDelete.add(staleCompoundCommit);
        maybeTriggerConsistencyCheck();
    }

    private void maybeTriggerConsistencyCheck() {
        if (isRunning.get() && pendingCommitsToDelete.isEmpty() == false && consistencyCheckPermit.tryAcquire()) {
            List<StaleCompoundCommit> commitsToDelete = new ArrayList<>();
            StaleCompoundCommit commitToDelete;
            while ((commitToDelete = pendingCommitsToDelete.poll()) != null) {
                commitsToDelete.add(commitToDelete);
            }
            var validationTask = new ValidateCompoundCommitsDeletionTask(
                Collections.unmodifiableList(commitsToDelete),
                ActionListener.runBefore(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        maybeTriggerConsistencyCheck();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // The validation task runs until it gets a valid cluster state or until the service closes (when the node stops)
                        // meaning that we won't trigger a new consistency check to validate the commit deletion.
                        logger.warn("Unable to validate cluster state consistency to delete stale compound commits", e);
                    }
                }, consistencyCheckPermit::release)
            );
            validationTask.run();
        }
    }

    private class ValidateCompoundCommitsDeletionTask extends RetryableAction<Void> {

        private ValidateCompoundCommitsDeletionTask(List<StaleCompoundCommit> commitsToDelete, ActionListener<Void> listener) {
            super(
                org.apache.logging.log4j.LogManager.getLogger(StatelessCommitCleaner.class),
                threadPool,
                TimeValue.timeValueMillis(50),
                TimeValue.timeValueSeconds(5),
                TimeValue.MAX_VALUE,
                listener.map(v -> {
                    deleteStaleCommitsIfIndexDeletedOrNodeIsStillPrimary(commitsToDelete);
                    return null;
                }),
                threadPool.executor(ThreadPool.Names.GENERIC)
            );
        }

        @Override
        public void tryAction(ActionListener<Void> listener) {
            consistencyService.ensureClusterStateConsistentWithRootBlob(listener, TimeValue.MAX_VALUE);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return isRunning.get();
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.routingTableChanged()) {
            handlePendingCommitsOfRelocatingPrimariesToDelete(event.state());
        }
    }

    private void handlePendingCommitsOfRelocatingPrimariesToDelete() {
        handlePendingCommitsOfRelocatingPrimariesToDelete(consistencyService.state());
    }

    private void handlePendingCommitsOfRelocatingPrimariesToDelete(ClusterState state) {
        if (pendingCommitsOfRelocatingPrimariesToDelete.isEmpty() == false && relocatingPrimariesCheckPermit.tryAcquire()) {
            threadPool.generic().execute(() -> {
                boolean successful = false;
                try {
                    // Move commits from pending relocating primaries to the main list for deletion, if their primaries have started
                    boolean anyMoved = false;
                    for (var it = pendingCommitsOfRelocatingPrimariesToDelete.iterator(); it.hasNext();) {
                        var commit = it.next();
                        try {
                            // Can throw if the index or shard is not there anymore
                            if (state.routingTable().shardRoutingTable(commit.shardId()).primaryShard().started()) {
                                pendingCommitsToDelete.add(commit);
                                anyMoved = true;
                                it.remove();
                            }
                        } catch (IndexNotFoundException e) {
                            logger.trace(() -> Strings.format("Exception handling commit [%s] to delete of relocating primary", commit), e);
                            pendingCommitsToDelete.add(commit);
                            anyMoved = true;
                            it.remove();
                        }
                    }
                    if (anyMoved) {
                        maybeTriggerConsistencyCheck();
                    }
                    successful = true;
                } catch (Exception e) {
                    assert false : e;
                    logger.warn("Exception while iterating pending commits to delete of relocating primaries", e);
                } finally {
                    relocatingPrimariesCheckPermit.release();
                    if (successful && consistencyService.state().version() > state.version()) {
                        // Check again with latest cluster state in case the list is still not empty
                        handlePendingCommitsOfRelocatingPrimariesToDelete();
                    }
                }
            });
        }
    }

    private void deleteStaleCommitsIfIndexDeletedOrNodeIsStillPrimary(List<StaleCompoundCommit> commitsToDelete) {
        ClusterState state = consistencyService.state();
        boolean addedPendingCommitsOfRelocatingPrimariesToDelete = false;
        for (StaleCompoundCommit staleCompoundCommit : commitsToDelete) {
            ShardDeletionState shardDeletionState = getShardDeletionState(
                staleCompoundCommit.shardId(),
                staleCompoundCommit.allocationPrimaryTerm(),
                state
            );
            switch (shardDeletionState) {
                case LOCAL_NODE_IS_PRIMARY:
                case INDEX_DELETED:
                    logger.debug("Delete shard file {}", staleCompoundCommit);
                    objectStoreService.asyncDeleteShardFile(staleCompoundCommit);
                    break;
                case LOCAL_NODE_IS_RELOCATING_TARGET_PRIMARY:
                    logger.debug("Reinserting commit {} for pending deletion", staleCompoundCommit);
                    pendingCommitsOfRelocatingPrimariesToDelete.add(staleCompoundCommit);
                    addedPendingCommitsOfRelocatingPrimariesToDelete = true;
                    break;
                case LOCAL_NODE_IS_NOT_PRIMARY:
                    logger.debug("Unable to delete commit {} since the primary shard was re-assigned", staleCompoundCommit);
                    break;
                default:
                    logger.warn("Unable to delete commit {} due to unrecognized shard state {}", staleCompoundCommit, shardDeletionState);
                    assert false : "unrecognized shard state " + shardDeletionState;
                    break;
            }
        }
        if (addedPendingCommitsOfRelocatingPrimariesToDelete) {
            handlePendingCommitsOfRelocatingPrimariesToDelete();
        }
    }

    private enum ShardDeletionState {
        INDEX_DELETED,
        LOCAL_NODE_IS_PRIMARY,
        LOCAL_NODE_IS_RELOCATING_TARGET_PRIMARY,
        LOCAL_NODE_IS_NOT_PRIMARY
    }

    private ShardDeletionState getShardDeletionState(ShardId shardId, long allocationPrimaryTerm, ClusterState state) {
        var indexRoutingTable = state.routingTable().index(shardId.getIndex());
        if (indexRoutingTable == null) {
            return ShardDeletionState.INDEX_DELETED;
        }

        var primaryShard = indexRoutingTable.shard(shardId.getId()).primaryShard();
        var localNode = state.nodes().getLocalNode();
        var currentPrimaryTerm = state.metadata().index(shardId.getIndex()).primaryTerm(shardId.getId());

        if (currentPrimaryTerm == allocationPrimaryTerm) {
            if (localNode.getId().equals(primaryShard.currentNodeId())) {
                return ShardDeletionState.LOCAL_NODE_IS_PRIMARY;
            } else if (primaryShard.relocating() && localNode.getId().equals(primaryShard.relocatingNodeId())) {
                // Primary relocation underway. The primary shard routing is RELOCATING and the target shard routing should be INITIALIZING.
                return ShardDeletionState.LOCAL_NODE_IS_RELOCATING_TARGET_PRIMARY;
            }
        }
        return ShardDeletionState.LOCAL_NODE_IS_NOT_PRIMARY;
    }

    @Override
    protected void doStart() {
        consistencyService.clusterService().addListener(this);
    }

    @Override
    protected void doStop() {
        consistencyService.clusterService().removeListener(this);
    }

    @Override
    protected void doClose() {
        isRunning.set(false);
    }

}
