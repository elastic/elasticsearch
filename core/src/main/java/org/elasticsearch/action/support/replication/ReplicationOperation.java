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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ReplicationOperation<
            Request extends ReplicationRequest<Request>,
            ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
            PrimaryResultT extends ReplicationOperation.PrimaryResult<ReplicaRequest>
        > {
    private final ESLogger logger;
    private final Request request;
    private final Supplier<ClusterState> clusterStateSupplier;
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
    private final AtomicInteger pendingShards = new AtomicInteger();
    private final AtomicInteger successfulShards = new AtomicInteger();
    private final boolean executeOnReplicas;
    private final Primary<Request, ReplicaRequest, PrimaryResultT> primary;
    private final Replicas<ReplicaRequest> replicasProxy;
    private final AtomicBoolean finished = new AtomicBoolean();
    protected final ActionListener<PrimaryResultT> resultListener;

    private volatile PrimaryResultT primaryResult = null;

    private final List<ReplicationResponse.ShardInfo.Failure> shardReplicaFailures = Collections.synchronizedList(new ArrayList<>());

    public ReplicationOperation(Request request, Primary<Request, ReplicaRequest, PrimaryResultT> primary,
                                ActionListener<PrimaryResultT> listener,
                                boolean executeOnReplicas, Replicas<ReplicaRequest> replicas,
                                Supplier<ClusterState> clusterStateSupplier, ESLogger logger, String opType) {
        this.executeOnReplicas = executeOnReplicas;
        this.replicasProxy = replicas;
        this.primary = primary;
        this.resultListener = listener;
        this.logger = logger;
        this.request = request;
        this.clusterStateSupplier = clusterStateSupplier;
        this.opType = opType;
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
        pendingShards.incrementAndGet();
        primaryResult = primary.perform(request);
        final ReplicaRequest replicaRequest = primaryResult.replicaRequest();
        assert replicaRequest.primaryTerm() > 0 : "replicaRequest doesn't have a primary term";
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] op [{}] completed on primary for request [{}]", primaryId, opType, request);
        }

        performOnReplicas(primaryId, replicaRequest);

        successfulShards.incrementAndGet();
        decPendingAndFinishIfNeeded();
    }

    private void performOnReplicas(ShardId primaryId, ReplicaRequest replicaRequest) {
        // we have to get a new state after successfully indexing into the primary in order to honour recovery semantics.
        // we have to make sure that every operation indexed into the primary after recovery start will also be replicated
        // to the recovery target. If we use an old cluster state, we may miss a relocation that has started since then.
        // If the index gets deleted after primary operation, we skip replication
        final List<ShardRouting> shards = getShards(primaryId, clusterStateSupplier.get());
        final String localNodeId = primary.routingEntry().currentNodeId();
        for (final ShardRouting shard : shards) {
            if (executeOnReplicas == false || shard.unassigned()) {
                if (shard.primary() == false) {
                    totalShards.incrementAndGet();
                }
                continue;
            }

            if (shard.currentNodeId().equals(localNodeId) == false) {
                performOnReplica(shard, replicaRequest);
            }

            if (shard.relocating() && shard.relocatingNodeId().equals(localNodeId) == false) {
                performOnReplica(shard.getTargetRelocatingShard(), replicaRequest);
            }
        }
    }

    private void performOnReplica(final ShardRouting shard, final ReplicaRequest replicaRequest) {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] sending op [{}] to replica {} for request [{}]", shard.shardId(), opType, shard, replicaRequest);
        }

        totalShards.incrementAndGet();
        pendingShards.incrementAndGet();
        replicasProxy.performOn(shard, replicaRequest, new ActionListener<TransportResponse.Empty>() {
            @Override
            public void onResponse(TransportResponse.Empty empty) {
                successfulShards.incrementAndGet();
                decPendingAndFinishIfNeeded();
            }

            @Override
            public void onFailure(Exception replicaException) {
                logger.trace("[{}] failure while performing [{}] on replica {}, request [{}]", replicaException, shard.shardId(), opType,
                    shard, replicaRequest);
                if (ignoreReplicaException(replicaException)) {
                    decPendingAndFinishIfNeeded();
                } else {
                    RestStatus restStatus = ExceptionsHelper.status(replicaException);
                    shardReplicaFailures.add(new ReplicationResponse.ShardInfo.Failure(
                        shard.shardId(), shard.currentNodeId(), replicaException, restStatus, false));
                    String message = String.format(Locale.ROOT, "failed to perform %s on replica %s", opType, shard);
                    logger.warn("[{}] {}", replicaException, shard.shardId(), message);
                    replicasProxy.failShard(shard, replicaRequest.primaryTerm(), message, replicaException,
                        ReplicationOperation.this::decPendingAndFinishIfNeeded,
                        ReplicationOperation.this::onPrimaryDemoted,
                        throwable -> decPendingAndFinishIfNeeded()
                    );
                }
            }
        });
    }

    private void onPrimaryDemoted(Exception demotionFailure) {
        String primaryFail = String.format(Locale.ROOT,
            "primary shard [%s] was demoted while failing replica shard",
            primary.routingEntry());
        // we are no longer the primary, fail ourselves and start over
        primary.failShard(primaryFail, demotionFailure);
        finishAsFailed(new RetryOnPrimaryException(primary.routingEntry().shardId(), primaryFail, demotionFailure));
    }

    /**
     * Checks whether we can perform a write based on the required active shard count setting.
     * Returns **null* if OK to proceed, or a string describing the reason to stop
     */
    protected String checkActiveShardCount() {
        final ShardId shardId = primary.routingEntry().shardId();
        final String indexName = shardId.getIndexName();
        final ClusterState state = clusterStateSupplier.get();
        assert state != null : "replication operation must have access to the cluster state";
        final ActiveShardCount waitForActiveShards = request.waitForActiveShards();
        if (waitForActiveShards == ActiveShardCount.NONE) {
            return null;  // not waiting for any shards
        }
        IndexRoutingTable indexRoutingTable = state.getRoutingTable().index(indexName);
        if (indexRoutingTable == null) {
            logger.trace("[{}] index not found in the routing table", shardId);
            return "Index " + indexName + " not found in the routing table";
        }
        IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId.getId());
        if (shardRoutingTable == null) {
            logger.trace("[{}] shard not found in the routing table", shardId);
            return "Shard " + shardId + " not found in the routing table";
        }
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

    protected List<ShardRouting> getShards(ShardId shardId, ClusterState state) {
        // can be null if the index is deleted / closed on us..
        final IndexShardRoutingTable shardRoutingTable = state.getRoutingTable().shardRoutingTableOrNull(shardId);
        List<ShardRouting> shards = shardRoutingTable == null ? Collections.emptyList() : shardRoutingTable.shards();
        return shards;
    }

    private void decPendingAndFinishIfNeeded() {
        assert pendingShards.get() > 0;
        if (pendingShards.decrementAndGet() == 0) {
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
     * Should an exception be ignored when the operation is performed on the replica.
     */
    public static boolean ignoreReplicaException(Exception e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            return true;
        }
        // on version conflict or document missing, it means
        // that a new change has crept into the replica, and it's fine
        if (isConflictException(e)) {
            return true;
        }
        return false;
    }

    public static boolean isConflictException(Throwable t) {
        final Throwable cause = ExceptionsHelper.unwrapCause(t);
        // on version conflict or document missing, it means
        // that a new change has crept into the replica, and it's fine
        return cause instanceof VersionConflictEngineException;
    }


    public interface Primary<
                Request extends ReplicationRequest<Request>,
                ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
                PrimaryResultT extends PrimaryResult<ReplicaRequest>
            > {

        /**
         * routing entry for this primary
         */
        ShardRouting routingEntry();

        /**
         * fail the primary, typically due to the fact that the operation has learned the primary has been demoted by the master
         */
        void failShard(String message, Exception exception);

        /**
         * Performs the given request on this primary. Yes, this returns as soon as it can with the request for the replicas and calls a
         * listener when the primary request is completed. Yes, the primary request might complete before the method returns. Yes, it might
         * also complete after. Deal with it.
         *
         * @param request the request to perform
         * @return the request to send to the repicas
         */
        PrimaryResultT perform(Request request) throws Exception;

    }

    public interface Replicas<ReplicaRequest extends ReplicationRequest<ReplicaRequest>> {

        /**
         * performs the the given request on the specified replica
         *
         * @param replica        {@link ShardRouting} of the shard this request should be executed on
         * @param replicaRequest operation to peform
         * @param listener       a callback to call once the operation has been complicated, either successfully or with an error.
         */
        void performOn(ShardRouting replica, ReplicaRequest replicaRequest, ActionListener<TransportResponse.Empty> listener);

        /**
         * Fail the specified shard, removing it from the current set of active shards
         * @param replica          shard to fail
         * @param primaryTerm      the primary term of the primary shard when requesting the failure
         * @param message          a (short) description of the reason
         * @param exception        the original exception which caused the ReplicationOperation to request the shard to be failed
         * @param onSuccess        a callback to call when the shard has been successfully removed from the active set.
         * @param onPrimaryDemoted a callback to call when the shard can not be failed because the current primary has been demoted
*                         by the master.
         * @param onIgnoredFailure a callback to call when failing a shard has failed, but it that failure can be safely ignored and the
         */
        void failShard(ShardRouting replica, long primaryTerm, String message, Exception exception, Runnable onSuccess,
                       Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure);
    }

    public static class RetryOnPrimaryException extends ElasticsearchException {
        public RetryOnPrimaryException(ShardId shardId, String msg) {
            this(shardId, msg, null);
        }

        public RetryOnPrimaryException(ShardId shardId, String msg, Throwable cause) {
            super(msg, cause);
            setShard(shardId);
        }

        public RetryOnPrimaryException(StreamInput in) throws IOException {
            super(in);
        }
    }

    public interface PrimaryResult<R extends ReplicationRequest<R>> {

        R replicaRequest();

        void setShardInfo(ReplicationResponse.ShardInfo shardInfo);
    }
}
