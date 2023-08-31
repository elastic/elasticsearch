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

import co.elastic.elasticsearch.stateless.ObjectStoreService;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.TimeValue;
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

public class StatelessCommitCleaner extends AbstractLifecycleComponent {
    private final Logger logger = LogManager.getLogger(StatelessCommitCleaner.class);

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final StatelessClusterConsistencyService consistencyService;
    private final ThreadPool threadPool;
    private final ObjectStoreService objectStoreService;
    private final ConcurrentLinkedQueue<StaleCompoundCommit> pendingCommitsToDelete = new ConcurrentLinkedQueue<>();
    private final Semaphore consistencyCheckPermit = new Semaphore(1);

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
                    deleteStaleCommitsIfNodeIsStillPrimary(commitsToDelete);
                    return null;
                }),
                ThreadPool.Names.GENERIC
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

    private void deleteStaleCommitsIfNodeIsStillPrimary(List<StaleCompoundCommit> commitsToDelete) {
        ClusterState state = consistencyService.state();
        for (StaleCompoundCommit staleCompoundCommit : commitsToDelete) {
            if (isLocalNodePrimaryForShard(staleCompoundCommit.shardId(), staleCompoundCommit.allocationPrimaryTerm(), state)) {
                logger.debug("Delete shard file {}", staleCompoundCommit);
                objectStoreService.asyncDeleteShardFile(staleCompoundCommit);
            } else {
                logger.debug("Unable to delete commit {} since the primary shard was re-assigned", staleCompoundCommit);
            }
        }
    }

    private boolean isLocalNodePrimaryForShard(ShardId shardId, long allocationPrimaryTerm, ClusterState state) {
        var indexRoutingTable = state.routingTable().index(shardId.getIndex());
        if (indexRoutingTable == null) {
            return false;
        }

        var primaryShard = indexRoutingTable.shard(shardId.getId()).primaryShard();
        var localNode = state.nodes().getLocalNode();
        var currentPrimaryTerm = state.metadata().index(shardId.getIndex()).primaryTerm(shardId.getId());

        return localNode.getId().equals(primaryShard.currentNodeId()) && currentPrimaryTerm == allocationPrimaryTerm;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        isRunning.set(false);
    }

}
