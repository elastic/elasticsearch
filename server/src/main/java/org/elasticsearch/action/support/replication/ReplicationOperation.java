/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

public class ReplicationOperation<
            Request extends ReplicationRequest<Request>,
            ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
            PrimaryResultT extends ReplicationOperation.PrimaryResult<ReplicaRequest>
        > {
    private final Logger logger;
    private final Request request;
    private final String opType;
    private final AtomicInteger totalShards = new AtomicInteger();
    /**
     * The number of pending sub-operations in this operation. This is incremented when the following operations start and decremented when
     * they complete:
     * <ul>
     * <li>The operation on the primary</li>
     * <li>The operation on each replica</li>
     * <li>Coordination of the operation as a whole. This prevents the operation from terminating early if we haven't started any replica
     * operations and the primary finishes.</li>
     * </ul>
     */
    private final AtomicInteger pendingActions = new AtomicInteger();
    private final AtomicInteger successfulShards = new AtomicInteger();
    private final Primary<Request, ReplicaRequest, PrimaryResultT> primary;
    private final Replicas<ReplicaRequest> replicasProxy;
    private final AtomicBoolean finished = new AtomicBoolean();
    private final long primaryTerm;

    // exposed for tests
    private final ActionListener<PrimaryResultT> resultListener;

    private volatile PrimaryResultT primaryResult = null;

    private final List<ReplicationResponse.ShardInfo.Failure> shardReplicaFailures = Collections.synchronizedList(new ArrayList<>());

    public ReplicationOperation(Request request, Primary<Request, ReplicaRequest, PrimaryResultT> primary,
                                ActionListener<PrimaryResultT> listener,
                                Replicas<ReplicaRequest> replicas,
                                Logger logger, String opType, long primaryTerm) {
        this.replicasProxy = replicas;
        this.primary = primary;
        this.resultListener = listener;
        this.logger = logger;
        this.request = request;
        this.opType = opType;
        this.primaryTerm = primaryTerm;
    }

    public void execute() throws Exception {
        final String activeShardCountFailure = checkActiveShardCount();
        final ShardRouting primaryRouting = primary.routingEntry();
        final ShardId primaryId = primaryRouting.shardId();
        if (activeShardCountFailure != null) {
            finishAsFailed(new UnavailableShardsException(primaryId,
                "{} Timeout: [{}], request: [{}]", activeShardCountFailure, request.timeout(), request));
            return;
        }

        totalShards.incrementAndGet();
        pendingActions.incrementAndGet(); // increase by 1 until we finish all primary coordination
        primary.perform(request, ActionListener.wrap(this::handlePrimaryResult, resultListener::onFailure));
    }

    private void handlePrimaryResult(final PrimaryResultT primaryResult) {
        this.primaryResult = primaryResult;
        final ReplicaRequest replicaRequest = primaryResult.replicaRequest();
        if (replicaRequest != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("[{}] op [{}] completed on primary for request [{}]", primary.routingEntry().shardId(), opType, request);
            }
            // we have to get the replication group after successfully indexing into the primary in order to honour recovery semantics.
            // we have to make sure that every operation indexed into the primary after recovery start will also be replicated
            // to the recovery target. If we used an old replication group, we may miss a recovery that has started since then.
            // we also have to make sure to get the global checkpoint before the replication group, to ensure that the global checkpoint
            // is valid for this replication group. If we would sample in the reverse, the global checkpoint might be based on a subset
            // of the sampled replication group, and advanced further than what the given replication group would allow it to.
            // This would entail that some shards could learn about a global checkpoint that would be higher than its local checkpoint.
            final long globalCheckpoint = primary.computedGlobalCheckpoint();
            // we have to capture the max_seq_no_of_updates after this request was completed on the primary to make sure the value of
            // max_seq_no_of_updates on replica when this request is executed is at least the value on the primary when it was executed
            // on.
            final long maxSeqNoOfUpdatesOrDeletes = primary.maxSeqNoOfUpdatesOrDeletes();
            assert maxSeqNoOfUpdatesOrDeletes != SequenceNumbers.UNASSIGNED_SEQ_NO : "seqno_of_updates still uninitialized";
            final ReplicationGroup replicationGroup = primary.getReplicationGroup();
            markUnavailableShardsAsStale(replicaRequest, replicationGroup);
            performOnReplicas(replicaRequest, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, replicationGroup);
        }
        primaryResult.runPostReplicationActions(new ActionListener<>() {

            @Override
            public void onResponse(Void aVoid) {
                successfulShards.incrementAndGet();
                try {
                    updateCheckPoints(primary.routingEntry(), primary::localCheckpoint, primary::globalCheckpoint);
                } finally {
                    decPendingAndFinishIfNeeded();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.trace("[{}] op [{}] post replication actions failed for [{}]", primary.routingEntry().shardId(), opType, request);
                // TODO: fail shard? This will otherwise have the local / global checkpoint info lagging, or possibly have replicas
                // go out of sync with the primary
                finishAsFailed(e);
            }
        });
    }

    private void markUnavailableShardsAsStale(ReplicaRequest replicaRequest, ReplicationGroup replicationGroup) {
        // if inSyncAllocationIds contains allocation ids of shards that don't exist in RoutingTable, mark copies as stale
        for (String allocationId : replicationGroup.getUnavailableInSyncShards()) {
            pendingActions.incrementAndGet();
            replicasProxy.markShardCopyAsStaleIfNeeded(replicaRequest.shardId(), allocationId, primaryTerm,
                ActionListener.wrap(r -> decPendingAndFinishIfNeeded(), ReplicationOperation.this::onNoLongerPrimary));
        }
    }

    private void performOnReplicas(final ReplicaRequest replicaRequest, final long globalCheckpoint,
                                   final long maxSeqNoOfUpdatesOrDeletes, final ReplicationGroup replicationGroup) {
        // for total stats, add number of unassigned shards and
        // number of initializing shards that are not ready yet to receive operations (recovery has not opened engine yet on the target)
        totalShards.addAndGet(replicationGroup.getSkippedShards().size());

        final ShardRouting primaryRouting = primary.routingEntry();

        for (final ShardRouting shard : replicationGroup.getReplicationTargets()) {
            if (shard.isSameAllocation(primaryRouting) == false) {
                performOnReplica(shard, replicaRequest, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes);
            }
        }
    }

    private void performOnReplica(final ShardRouting shard, final ReplicaRequest replicaRequest,
                                  final long globalCheckpoint, final long maxSeqNoOfUpdatesOrDeletes) {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] sending op [{}] to replica {} for request [{}]", shard.shardId(), opType, shard, replicaRequest);
        }

        totalShards.incrementAndGet();
        pendingActions.incrementAndGet();
        replicasProxy.performOn(shard, replicaRequest, primaryTerm, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes,
            new ActionListener<>() {
                @Override
                public void onResponse(ReplicaResponse response) {
                    successfulShards.incrementAndGet();
                    try {
                        updateCheckPoints(shard, response::localCheckpoint, response::globalCheckpoint);
                    } finally {
                        decPendingAndFinishIfNeeded();
                    }
                }

                @Override
                public void onFailure(Exception replicaException) {
                    logger.trace(() -> new ParameterizedMessage(
                        "[{}] failure while performing [{}] on replica {}, request [{}]",
                        shard.shardId(), opType, shard, replicaRequest), replicaException);
                    // Only report "critical" exceptions - TODO: Reach out to the master node to get the latest shard state then report.
                    if (TransportActions.isShardNotAvailableException(replicaException) == false) {
                        RestStatus restStatus = ExceptionsHelper.status(replicaException);
                        shardReplicaFailures.add(new ReplicationResponse.ShardInfo.Failure(
                            shard.shardId(), shard.currentNodeId(), replicaException, restStatus, false));
                    }
                    String message = String.format(Locale.ROOT, "failed to perform %s on replica %s", opType, shard);
                    replicasProxy.failShardIfNeeded(shard, primaryTerm, message, replicaException,
                        ActionListener.wrap(r -> decPendingAndFinishIfNeeded(), ReplicationOperation.this::onNoLongerPrimary));
                }

                @Override
                public String toString() {
                    return "[" + replicaRequest + "][" + shard + "]";
                }
            });
    }

    private void updateCheckPoints(ShardRouting shard, LongSupplier localCheckpointSupplier, LongSupplier globalCheckpointSupplier) {
        try {
            primary.updateLocalCheckpointForShard(shard.allocationId().getId(), localCheckpointSupplier.getAsLong());
            primary.updateGlobalCheckpointForShard(shard.allocationId().getId(), globalCheckpointSupplier.getAsLong());
        } catch (final AlreadyClosedException e) {
            // the index was deleted or this shard was never activated after a relocation; fall through and finish normally
        } catch (final Exception e) {
            // fail the primary but fall through and let the rest of operation processing complete
            final String message = String.format(Locale.ROOT, "primary failed updating local checkpoint for replica %s", shard);
            primary.failShard(message, e);
        }
    }

    private void onNoLongerPrimary(Exception failure) {
        final Throwable cause = ExceptionsHelper.unwrapCause(failure);
        final boolean nodeIsClosing = cause instanceof NodeClosedException;
        final String message;
        if (nodeIsClosing) {
            message = String.format(Locale.ROOT,
                "node with primary [%s] is shutting down while failing replica shard", primary.routingEntry());
            // We prefer not to fail the primary to avoid unnecessary warning log
            // when the node with the primary shard is gracefully shutting down.
        } else {
            if (Assertions.ENABLED) {
                if (failure instanceof ShardStateAction.NoLongerPrimaryShardException == false) {
                    throw new AssertionError("unexpected failure", failure);
                }
            }
            // we are no longer the primary, fail ourselves and start over
            message = String.format(Locale.ROOT, "primary shard [%s] was demoted while failing replica shard", primary.routingEntry());
            primary.failShard(message, failure);
        }
        finishAsFailed(new RetryOnPrimaryException(primary.routingEntry().shardId(), message, failure));
    }

    /**
     * Checks whether we can perform a write based on the required active shard count setting.
     * Returns **null* if OK to proceed, or a string describing the reason to stop
     */
    protected String checkActiveShardCount() {
        final ShardId shardId = primary.routingEntry().shardId();
        final ActiveShardCount waitForActiveShards = request.waitForActiveShards();
        if (waitForActiveShards == ActiveShardCount.NONE) {
            return null;  // not waiting for any shards
        }
        final IndexShardRoutingTable shardRoutingTable = primary.getReplicationGroup().getRoutingTable();
        if (waitForActiveShards.enoughShardsActive(shardRoutingTable)) {
            return null;
        } else {
            final String resolvedShards = waitForActiveShards == ActiveShardCount.ALL ? Integer.toString(shardRoutingTable.shards().size())
                                              : waitForActiveShards.toString();
            logger.trace("[{}] not enough active copies to meet shard count of [{}] (have {}, needed {}), scheduling a retry. op [{}], " +
                         "request [{}]", shardId, waitForActiveShards, shardRoutingTable.activeShards().size(),
                         resolvedShards, opType, request);
            return "Not enough active copies to meet shard count of [" + waitForActiveShards + "] (have " +
                       shardRoutingTable.activeShards().size() + ", needed " + resolvedShards + ").";
        }
    }

    private void decPendingAndFinishIfNeeded() {
        assert pendingActions.get() > 0 : "pending action count goes below 0 for request [" + request + "]";
        if (pendingActions.decrementAndGet() == 0) {
            finish();
        }
    }

    private void finish() {
        if (finished.compareAndSet(false, true)) {
            final ReplicationResponse.ShardInfo.Failure[] failuresArray;
            if (shardReplicaFailures.isEmpty()) {
                failuresArray = ReplicationResponse.EMPTY;
            } else {
                failuresArray = new ReplicationResponse.ShardInfo.Failure[shardReplicaFailures.size()];
                shardReplicaFailures.toArray(failuresArray);
            }
            primaryResult.setShardInfo(new ReplicationResponse.ShardInfo(
                    totalShards.get(),
                    successfulShards.get(),
                    failuresArray
                )
            );
            resultListener.onResponse(primaryResult);
        }
    }

    private void finishAsFailed(Exception exception) {
        if (finished.compareAndSet(false, true)) {
            resultListener.onFailure(exception);
        }
    }

    /**
     * An encapsulation of an operation that is to be performed on the primary shard
     */
    public interface Primary<
                RequestT extends ReplicationRequest<RequestT>,
                ReplicaRequestT extends ReplicationRequest<ReplicaRequestT>,
                PrimaryResultT extends PrimaryResult<ReplicaRequestT>
            > {

        /**
         * routing entry for this primary
         */
        ShardRouting routingEntry();

        /**
         * Fail the primary shard.
         *
         * @param message   the failure message
         * @param exception the exception that triggered the failure
         */
        void failShard(String message, Exception exception);

        /**
         * Performs the given request on this primary. Yes, this returns as soon as it can with the request for the replicas and calls a
         * listener when the primary request is completed. Yes, the primary request might complete before the method returns. Yes, it might
         * also complete after. Deal with it.
         *
         * @param request the request to perform
         * @param listener result listener
         */
        void perform(RequestT request, ActionListener<PrimaryResultT> listener);

        /**
         * Notifies the primary of a local checkpoint for the given allocation.
         *
         * Note: The primary will use this information to advance the global checkpoint if possible.
         *
         * @param allocationId allocation ID of the shard corresponding to the supplied local checkpoint
         * @param checkpoint the *local* checkpoint for the shard
         */
        void updateLocalCheckpointForShard(String allocationId, long checkpoint);

        /**
         * Update the local knowledge of the global checkpoint for the specified allocation ID.
         *
         * @param allocationId     the allocation ID to update the global checkpoint for
         * @param globalCheckpoint the global checkpoint
         */
        void updateGlobalCheckpointForShard(String allocationId, long globalCheckpoint);

        /**
         * Returns the persisted local checkpoint on the primary shard.
         *
         * @return the local checkpoint
         */
        long localCheckpoint();

        /**
         * Returns the global checkpoint computed on the primary shard.
         *
         * @return the computed global checkpoint
         */
        long computedGlobalCheckpoint();

        /**
         * Returns the persisted global checkpoint on the primary shard.
         *
         * @return the persisted global checkpoint
         */
        long globalCheckpoint();

        /**
         * Returns the maximum seq_no of updates (index operations overwrite Lucene) or deletes on the primary.
         * This value must be captured after the execution of a replication request on the primary is completed.
         */
        long maxSeqNoOfUpdatesOrDeletes();

        /**
         * Returns the current replication group on the primary shard
         *
         * @return the replication group
         */
        ReplicationGroup getReplicationGroup();
    }

    /**
     * An encapsulation of an operation that will be executed on the replica shards, if present.
     */
    public interface Replicas<RequestT extends ReplicationRequest<RequestT>> {

        /**
         * Performs the specified request on the specified replica.
         *
         * @param replica                    the shard this request should be executed on
         * @param replicaRequest             the operation to perform
         * @param primaryTerm                the primary term
         * @param globalCheckpoint           the global checkpoint on the primary
         * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates (index operations overwriting Lucene) or deletes on primary
         *                                   after this replication was executed on it.
         * @param listener                   callback for handling the response or failure
         */
        void performOn(ShardRouting replica, RequestT replicaRequest,
                       long primaryTerm, long globalCheckpoint, long maxSeqNoOfUpdatesOrDeletes, ActionListener<ReplicaResponse> listener);

        /**
         * Fail the specified shard if needed, removing it from the current set
         * of active shards. Whether a failure is needed is left up to the
         * implementation.
         *
         * @param replica      shard to fail
         * @param primaryTerm  the primary term
         * @param message      a (short) description of the reason
         * @param exception    the original exception which caused the ReplicationOperation to request the shard to be failed
         * @param listener     a listener that will be notified when the failing shard has been removed from the in-sync set
         */
        void failShardIfNeeded(ShardRouting replica, long primaryTerm, String message, Exception exception, ActionListener<Void> listener);

        /**
         * Marks shard copy as stale if needed, removing its allocation id from
         * the set of in-sync allocation ids. Whether marking as stale is needed
         * is left up to the implementation.
         *
         * @param shardId      shard id
         * @param allocationId allocation id to remove from the set of in-sync allocation ids
         * @param primaryTerm  the primary term
         * @param listener     a listener that will be notified when the failing shard has been removed from the in-sync set
         */
        void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm, ActionListener<Void> listener);
    }

    /**
     * An interface to encapsulate the metadata needed from replica shards when they respond to operations performed on them.
     */
    public interface ReplicaResponse {

        /**
         * The persisted local checkpoint for the shard.
         *
         * @return the persisted local checkpoint
         **/
        long localCheckpoint();

        /**
         * The persisted global checkpoint for the shard.
         *
         * @return the persisted global checkpoint
         **/
        long globalCheckpoint();

    }

    public static class RetryOnPrimaryException extends ElasticsearchException {
        RetryOnPrimaryException(ShardId shardId, String msg) {
            this(shardId, msg, null);
        }

        RetryOnPrimaryException(ShardId shardId, String msg, Throwable cause) {
            super(msg, cause);
            setShard(shardId);
        }

        public RetryOnPrimaryException(StreamInput in) throws IOException {
            super(in);
        }
    }

    public interface PrimaryResult<RequestT extends ReplicationRequest<RequestT>> {

        /**
         * @return null if no operation needs to be sent to a replica
         * (for example when the operation failed on the primary due to a parsing exception)
         */
        @Nullable RequestT replicaRequest();

        void setShardInfo(ReplicationResponse.ShardInfo shardInfo);

        /**
         * Run actions to be triggered post replication
         * @param listener calllback that is invoked after post replication actions have completed
         * */
        void runPostReplicationActions(ActionListener<Void> listener);
    }
}
