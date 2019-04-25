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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteReplicaRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ReplicaResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * Base class for requests that should be executed on a primary copy followed by replica copies.
 * Subclasses can provide implementation for primary and replica operations.
 *
 * The action samples the cluster state on the primary node for resolving the nodes with replica copies to perform replication.
 */
public abstract class TransportReplicationAction<
    Request extends ReplicationRequest<Request>,
    ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
    Response extends ReplicationResponse
    > extends TransportAction<ConcreteReplicaRequest<ReplicaRequest>, ReplicaResponse> {

    private final Logger logger;

    private final TransportService transportService;
    private final String executor;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final IndicesService indicesService;
    private final TransportRequestOptions transportOptions;
    private final boolean syncGlobalCheckpointAfterOperation;

    protected TransportReplicationAction(String actionName, ActionFilters actionFilters, TaskManager taskManager,
                                         TransportService transportService, String executor,
                                         Writeable.Reader<ReplicaRequest> replicaRequestReader,
                                         ClusterService clusterService, ThreadPool threadPool, IndicesService indicesService,
                                         TransportRequestOptions transportOptions, boolean syncGlobalCheckpointAfterOperation) {
        // TODO reorder constructor parameters
        super(actionName + "[r]", actionFilters, taskManager);
        this.logger = Loggers.getLogger(TransportReplicationAction.class, actionName);
        this.transportService = transportService;
        this.executor = executor;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indicesService = indicesService;
        this.transportOptions = transportOptions;
        this.syncGlobalCheckpointAfterOperation = syncGlobalCheckpointAfterOperation;

        // we must never reject on because of thread pool capacity on replicas
        transportService.registerRequestHandler(actionName + "[r]", executor, true, true,
            in -> new ConcreteReplicaRequest<>(replicaRequestReader, in),
            (replicaRequest, channel, task) -> execute(task, replicaRequest,
                new ChannelActionListener<>(channel, this.actionName, replicaRequest)));
    }

    @Override
    protected void doExecute(Task task, ConcreteReplicaRequest<ReplicaRequest> request,
                             ActionListener<ReplicaResponse> listener) {
        new AsyncReplicaAction(request, listener, (ReplicationTask) task).run();
    }

    protected abstract void acquireReplicaOperationPermit(IndexShard replica,
                                                          ReplicaRequest request,
                                                          ActionListener<Releasable> onAcquired,
                                                          long primaryTerm,
                                                          long globalCheckpoint,
                                                          long maxSeqNoOfUpdatesOrDeletes);

    /**
     * Synchronously execute the specified replica operation. This is done under a permit from
     * {@link IndexShard#acquireReplicaOperationPermit(long, long, long, ActionListener, String, Object)}.
     *
     * @param shardRequest the request to the replica shard
     * @param replica      the replica shard to perform the operation on
     */
    protected abstract ReplicaResult shardOperationOnReplica(ReplicaRequest shardRequest, IndexShard replica) throws Exception;

    void performOnReplica(ShardRouting replica, ReplicaRequest request, long primaryTerm, long globalCheckpoint,
                          long maxSeqNoOfUpdatesOrDeletes, ActionListener<ReplicationOperation.ReplicaResponse> listener) {
        String nodeId = replica.currentNodeId();
        final DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        if (node == null) {
            listener.onFailure(new NoNodeAvailableException("unknown node [" + nodeId + "]"));
            return;
        }
        final TransportReplicationAction.ConcreteReplicaRequest<ReplicaRequest> replicaRequest
            = new TransportReplicationAction.ConcreteReplicaRequest<>(
            request, replica.allocationId().getId(), primaryTerm, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes);
        final ActionListenerResponseHandler<TransportReplicationAction.ReplicaResponse> handler
            = new ActionListenerResponseHandler<>(listener, in -> {
            TransportReplicationAction.ReplicaResponse replicaResponse
                = new TransportReplicationAction.ReplicaResponse();
            replicaResponse.readFrom(in);
            return replicaResponse;
        });
        transportService.sendRequest(node, actionName, replicaRequest, transportOptions, handler);
    }

    public void performAndReplicate(Request request,
                                    IndexShard primaryShard,
                                    ReplicationOperation.Primary<Request, ReplicaRequest,
                                        PrimaryResult<ReplicaRequest, Response>> primary,
                                    ReplicationOperation.Replicas<ReplicaRequest> replicasProxy,
                                    ActionListener<Response> listener) throws Exception {

        assert primaryShard.getActiveOperationsCount() != 0 : "must perform shard operation under a permit";
        assert primaryShard.routingEntry().primary() : "should be primary";
        assert primaryShard.isRelocatedPrimary() == false : "should not be a relocated primary";

        final ActionListener<Response> globalCheckpointSyncingListener = ActionListener.wrap(response -> {
            if (syncGlobalCheckpointAfterOperation) {
                try {
                    primaryShard.maybeSyncGlobalCheckpoint("post-operation");
                } catch (final Exception e) {
                    // only log non-closed exceptions
                    if (ExceptionsHelper.unwrap(
                        e, AlreadyClosedException.class, IndexShardClosedException.class) == null) {
                        // intentionally swallow, a missed global checkpoint sync should not fail this operation
                        logger.info(new ParameterizedMessage(
                            "{} failed to execute post-operation global checkpoint sync", primaryShard.shardId()), e);
                    }
                }
            }
            listener.onResponse(response);
        }, listener::onFailure);

        new ReplicationOperation<>(request, primary,
            ActionListener.wrap(result -> result.respond(globalCheckpointSyncingListener), globalCheckpointSyncingListener::onFailure),
            replicasProxy, logger, actionName, primaryShard.getOperationPrimaryTerm()).execute();
    }

    public static class ReplicaResult {
        final Exception finalFailure;

        public ReplicaResult(Exception finalFailure) {
            this.finalFailure = finalFailure;
        }

        public ReplicaResult() {
            this(null);
        }

        public void respond(ActionListener<TransportResponse.Empty> listener) {
            if (finalFailure == null) {
                listener.onResponse(TransportResponse.Empty.INSTANCE);
            } else {
                listener.onFailure(finalFailure);
            }
        }
    }

    public static class RetryOnReplicaException extends ElasticsearchException {

        public RetryOnReplicaException(ShardId shardId, String msg) {
            super(msg);
            setShard(shardId);
        }

        public RetryOnReplicaException(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class ReplicaResponse extends ActionResponse implements ReplicationOperation.ReplicaResponse {
        private long localCheckpoint;
        private long globalCheckpoint;

        ReplicaResponse() {

        }

        public ReplicaResponse(long localCheckpoint, long globalCheckpoint) {
            /*
             * A replica should always know its own local checkpoints so this should always be a valid sequence number or the pre-6.0
             * checkpoint value when simulating responses to replication actions that pre-6.0 nodes are not aware of (e.g., the global
             * checkpoint background sync, and the primary/replica resync).
             */
            assert localCheckpoint != SequenceNumbers.UNASSIGNED_SEQ_NO;
            this.localCheckpoint = localCheckpoint;
            this.globalCheckpoint = globalCheckpoint;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            localCheckpoint = in.readZLong();
            globalCheckpoint = in.readZLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeZLong(localCheckpoint);
            out.writeZLong(globalCheckpoint);
        }

        @Override
        public long localCheckpoint() {
            return localCheckpoint;
        }

        @Override
        public long globalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReplicaResponse that = (ReplicaResponse) o;
            return localCheckpoint == that.localCheckpoint &&
                globalCheckpoint == that.globalCheckpoint;
        }

        @Override
        public int hashCode() {
            return Objects.hash(localCheckpoint, globalCheckpoint);
        }
    }

    protected static final class ConcreteReplicaRequest<R extends ReplicationRequest>
        extends TransportReroutedReplicationAction.ConcreteShardRequest<R> {

        private final long globalCheckpoint;
        private final long maxSeqNoOfUpdatesOrDeletes;

        public ConcreteReplicaRequest(Reader<R> requestReader, StreamInput in) throws IOException {
            super(requestReader, in);
            globalCheckpoint = in.readZLong();
            maxSeqNoOfUpdatesOrDeletes = in.readZLong();
        }

        public ConcreteReplicaRequest(final R request, final String targetAllocationID, final long primaryTerm,
                                      final long globalCheckpoint, final long maxSeqNoOfUpdatesOrDeletes) {
            super(request, targetAllocationID, primaryTerm);
            this.globalCheckpoint = globalCheckpoint;
            this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
        }

        @Override
        public void readFrom(StreamInput in) {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeZLong(globalCheckpoint);
            out.writeZLong(maxSeqNoOfUpdatesOrDeletes);
        }

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }

        public long getMaxSeqNoOfUpdatesOrDeletes() {
            return maxSeqNoOfUpdatesOrDeletes;
        }

        @Override
        public String toString() {
            return "ConcreteReplicaRequest{" +
                    "shardId=[" + getRequest().shardId() + ']' +
                    ", targetAllocationID=[" + getTargetAllocationID() + ']' +
                    ", primaryTerm=[" + getPrimaryTerm() + ']' +
                    ", request=[" + getRequest() + ']' +
                    ", globalCheckpoint=" + globalCheckpoint +
                    ", maxSeqNoOfUpdatesOrDeletes=" + maxSeqNoOfUpdatesOrDeletes +
                    '}';
        }
    }

    public static class PrimaryResult<ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
            Response extends ReplicationResponse>
            implements ReplicationOperation.PrimaryResult<ReplicaRequest> {
        final ReplicaRequest replicaRequest;
        public final Response finalResponseIfSuccessful;
        public final Exception finalFailure;

        /**
         * Result of executing a primary operation
         * expects <code>finalResponseIfSuccessful</code> or <code>finalFailure</code> to be not-null
         */
        public PrimaryResult(ReplicaRequest replicaRequest, Response finalResponseIfSuccessful, Exception finalFailure) {
            assert finalFailure != null ^ finalResponseIfSuccessful != null
                    : "either a response or a failure has to be not null, " +
                    "found [" + finalFailure + "] failure and ["+ finalResponseIfSuccessful + "] response";
            this.replicaRequest = replicaRequest;
            this.finalResponseIfSuccessful = finalResponseIfSuccessful;
            this.finalFailure = finalFailure;
        }

        public PrimaryResult(ReplicaRequest replicaRequest, Response replicationResponse) {
            this(replicaRequest, replicationResponse, null);
        }

        @Override
        public ReplicaRequest replicaRequest() {
            return replicaRequest;
        }

        @Override
        public void setShardInfo(ReplicationResponse.ShardInfo shardInfo) {
            if (finalResponseIfSuccessful != null) {
                finalResponseIfSuccessful.setShardInfo(shardInfo);
            }
        }

        public void respond(ActionListener<Response> listener) {
            if (finalResponseIfSuccessful != null) {
                listener.onResponse(finalResponseIfSuccessful);
            } else {
                listener.onFailure(finalFailure);
            }
        }
    }


    private final class AsyncReplicaAction extends AbstractRunnable implements ActionListener<Releasable> {
        private final ActionListener<ReplicaResponse> onCompletionListener;
        private final IndexShard replica;
        /**
         * The task on the node with the replica shard.
         */
        private final ReplicationTask task;
        // important: we pass null as a timeout as failing a replica is
        // something we want to avoid at all costs
        private final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
        private final ConcreteReplicaRequest<ReplicaRequest> replicaRequest;

        AsyncReplicaAction(ConcreteReplicaRequest<ReplicaRequest> replicaRequest, ActionListener<ReplicaResponse> onCompletionListener,
                           ReplicationTask task) {
            this.replicaRequest = replicaRequest;
            this.onCompletionListener = onCompletionListener;
            this.task = task;
            final ShardId shardId = replicaRequest.getRequest().shardId();
            assert shardId != null : "request shardId must be set";
            this.replica = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        }

        @Override
        public void onResponse(Releasable releasable) {
            try {
                assert replica.getActiveOperationsCount() != 0 : "must perform shard operation under a permit";
                final ReplicaResult replicaResult = shardOperationOnReplica(replicaRequest.getRequest(), replica);
                releasable.close(); // release shard operation lock before responding to caller
                final ReplicaResponse response =
                    new ReplicaResponse(replica.getLocalCheckpoint(), replica.getGlobalCheckpoint());
                replicaResult.respond(new ResponseListener(response));
            } catch (final Exception e) {
                Releasables.closeWhileHandlingException(releasable); // release shard operation lock before responding to caller
                AsyncReplicaAction.this.onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof RetryOnReplicaException) {
                logger.trace(() -> new ParameterizedMessage("retrying [{}]", replicaRequest), e);
                replicaRequest.getRequest().onRetry();
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        // Forking a thread on local node via transport service so that custom transport service have an
                        // opportunity to execute custom logic before the replica operation begins
                        transportService.sendRequest(clusterService.localNode(), actionName,
                            replicaRequest,
                            new ActionListenerResponseHandler<>(onCompletionListener, in -> new ReplicaResponse()));
                    }

                    @Override
                    public void onClusterServiceClose() {
                        responseWithFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        throw new AssertionError("Cannot happen: there is not timeout");
                    }
                });
            } else {
                responseWithFailure(e);
            }
        }

        protected void responseWithFailure(Exception e) {
            setPhase("finished");
            onCompletionListener.onFailure(e);
        }

        @Override
        protected void doRun() {
            setPhase("replica");
            final String actualAllocationId = this.replica.routingEntry().allocationId().getId();
            if (actualAllocationId.equals(replicaRequest.getTargetAllocationID()) == false) {
                throw new ShardNotFoundException(this.replica.shardId(), "expected allocation id [{}] but found [{}]",
                    replicaRequest.getTargetAllocationID(), actualAllocationId);
            }
            acquireReplicaOperationPermit(replica, replicaRequest.getRequest(), this, replicaRequest.getPrimaryTerm(),
                replicaRequest.getGlobalCheckpoint(), replicaRequest.getMaxSeqNoOfUpdatesOrDeletes());
        }

        /**
         * Listens for the response on the replica and sends the response back to the primary.
         */
        private class ResponseListener implements ActionListener<TransportResponse.Empty> {
            private final ReplicaResponse replicaResponse;

            ResponseListener(ReplicaResponse replicaResponse) {
                this.replicaResponse = replicaResponse;
            }

            @Override
            public void onResponse(TransportResponse.Empty response) {
                logger.trace("completed [{}]", replicaRequest);
                setPhase("finished");
                onCompletionListener.onResponse(replicaResponse);
            }

            @Override
            public void onFailure(Exception e) {
                responseWithFailure(e);
            }
        }

        private void setPhase(String phase) {
            if (task != null) {
                task.setPhase(phase);
            }
        }
    }
}
