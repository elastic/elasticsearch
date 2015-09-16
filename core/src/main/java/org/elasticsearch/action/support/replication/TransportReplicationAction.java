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
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 */
public abstract class TransportReplicationAction<Request extends ReplicationRequest, ReplicaRequest extends ReplicationRequest, Response extends ActionWriteResponse> extends TransportAction<Request, Response> {

    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final IndicesService indicesService;
    protected final ShardStateAction shardStateAction;
    protected final WriteConsistencyLevel defaultWriteConsistencyLevel;
    protected final TransportRequestOptions transportOptions;
    protected final MappingUpdatedAction mappingUpdatedAction;

    final String transportReplicaAction;
    final String executor;
    final boolean checkWriteConsistency;

    protected TransportReplicationAction(Settings settings, String actionName, TransportService transportService,
                                         ClusterService clusterService, IndicesService indicesService,
                                         ThreadPool threadPool, ShardStateAction shardStateAction,
                                         MappingUpdatedAction mappingUpdatedAction, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request,
                                         Supplier<ReplicaRequest> replicaRequest, String executor) {
        super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardStateAction = shardStateAction;
        this.mappingUpdatedAction = mappingUpdatedAction;

        this.transportReplicaAction = actionName + "[r]";
        this.executor = executor;
        this.checkWriteConsistency = checkWriteConsistency();

        transportService.registerRequestHandler(actionName, request, ThreadPool.Names.SAME, new OperationTransportHandler());
        // we must never reject on because of thread pool capacity on replicas
        transportService.registerRequestHandler(transportReplicaAction, replicaRequest, executor, true, new ReplicaOperationTransportHandler());

        this.transportOptions = transportOptions();

        this.defaultWriteConsistencyLevel = WriteConsistencyLevel.fromString(settings.get("action.write_consistency", "quorum"));
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new PrimaryPhase(request, listener).run();
    }

    protected abstract Response newResponseInstance();

    /**
     * @return A tuple containing not null values, as first value the result of the primary operation and as second value
     * the request to be executed on the replica shards.
     */
    protected abstract Tuple<Response, ReplicaRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable;

    protected abstract void shardOperationOnReplica(ShardId shardId, ReplicaRequest shardRequest);

    protected abstract ShardIterator shards(ClusterState clusterState, InternalRequest request);

    protected abstract boolean checkWriteConsistency();

    protected ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.concreteIndex());
    }

    protected boolean resolveIndex() {
        return true;
    }

    /**
     * Resolves the request, by default doing nothing. Can be subclassed to do
     * additional processing or validation depending on the incoming request
     */
    protected void resolveRequest(ClusterState state, InternalRequest request, ActionListener<Response> listener) {
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    protected boolean retryPrimaryException(Throwable e) {
        return e.getClass() == RetryOnPrimaryException.class
                || TransportActions.isShardNotAvailableException(e);
    }

    /**
     * Should an exception be ignored when the operation is performed on the replica.
     */
    protected boolean ignoreReplicaException(Throwable e) {
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

    protected boolean isConflictException(Throwable e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        // on version conflict or document missing, it means
        // that a new change has crept into the replica, and it's fine
        if (cause instanceof VersionConflictEngineException) {
            return true;
        }
        if (cause instanceof DocumentAlreadyExistsException) {
            return true;
        }
        return false;
    }

    protected static class WriteResult<T extends ActionWriteResponse> {

        public final T response;
        public final Translog.Location location;

        public WriteResult(T response, Translog.Location location) {
            this.response = response;
            this.location = location;
        }

        @SuppressWarnings("unchecked")
        public <T extends ActionWriteResponse> T response() {
            // this sets total, pending and failed to 0 and this is ok, because we will embed this into the replica
            // request and not use it
            response.setShardInfo(new ActionWriteResponse.ShardInfo());
            return (T) response;
        }

    }

    class OperationTransportHandler implements TransportRequestHandler<Request> {
        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            execute(request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Throwable e1) {
                        logger.warn("Failed to send response for " + actionName, e1);
                    }
                }
            });
        }
    }

    class ReplicaOperationTransportHandler implements TransportRequestHandler<ReplicaRequest> {
        @Override
        public void messageReceived(final ReplicaRequest request, final TransportChannel channel) throws Exception {
            new AsyncReplicaAction(request, channel).run();
        }
    }

    public static class RetryOnReplicaException extends ElasticsearchException {

        public RetryOnReplicaException(ShardId shardId, String msg) {
            super(msg);
            setShard(shardId);
        }

        public RetryOnReplicaException(StreamInput in) throws IOException{
            super(in);
        }
    }

    private final class AsyncReplicaAction extends AbstractRunnable {
        private final ReplicaRequest request;
        private final TransportChannel channel;
        // important: we pass null as a timeout as failing a replica is
        // something we want to avoid at all costs
        private final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger);


        AsyncReplicaAction(ReplicaRequest request, TransportChannel channel) {
            this.request = request;
            this.channel = channel;
        }

        @Override
        public void onFailure(Throwable t) {
            if (t instanceof RetryOnReplicaException) {
                logger.trace("Retrying operation on replica", t);
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        threadPool.executor(executor).execute(AsyncReplicaAction.this);
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
                try {
                    failReplicaIfNeeded(request.internalShardId.getIndex(), request.internalShardId.id(), t);
                } catch (Throwable unexpected) {
                    logger.error("{} unexpected error while failing replica", request.internalShardId.id(), unexpected);
                } finally {
                    responseWithFailure(t);
                }
            }
        }

        protected void responseWithFailure(Throwable t) {
            try {
                channel.sendResponse(t);
            } catch (IOException responseException) {
                logger.warn("failed to send error message back to client for action [" + transportReplicaAction + "]", responseException);
                logger.warn("actual Exception", t);
            }
        }

        @Override
        protected void doRun() throws Exception {
            try (Releasable shardReference = getIndexShardOperationsCounter(request.internalShardId)) {
                shardOperationOnReplica(request.internalShardId, request);
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    protected class PrimaryOperationRequest {
        public final ShardId shardId;
        public final Request request;

        public PrimaryOperationRequest(int shardId, String index, Request request) {
            this.shardId = new ShardId(index, shardId);
            this.request = request;
        }
    }

    public static class RetryOnPrimaryException extends ElasticsearchException {
        public RetryOnPrimaryException(ShardId shardId, String msg) {
            super(msg);
            setShard(shardId);
        }

        public RetryOnPrimaryException(StreamInput in) throws IOException{
            super(in);
        }
    }

    /**
     * Responsible for performing all operations up to the point we start starting sending requests to replica shards.
     * Including forwarding the request to another node if the primary is not assigned locally.
     * <p/>
     * Note that as soon as we start sending request to replicas, state responsibility is transferred to {@link ReplicationPhase}
     */
    final class PrimaryPhase extends AbstractRunnable {
        private final ActionListener<Response> listener;
        private final InternalRequest internalRequest;
        private final ClusterStateObserver observer;
        private final AtomicBoolean finished = new AtomicBoolean(false);
        private volatile Releasable indexShardReference;


        PrimaryPhase(Request request, ActionListener<Response> listener) {
            this.internalRequest = new InternalRequest(request);
            this.listener = listener;
            this.observer = new ClusterStateObserver(clusterService, internalRequest.request().timeout(), logger);
        }

        @Override
        public void onFailure(Throwable e) {
            finishWithUnexpectedFailure(e);
        }

        @Override
        protected void doRun() {
            if (checkBlocks() == false) {
                return;
            }
            final ShardIterator shardIt = shards(observer.observedState(), internalRequest);
            final ShardRouting primary = resolvePrimary(shardIt);
            if (primary == null) {
                retryBecauseUnavailable(shardIt.shardId(), "No active shards.");
                return;
            }
            if (primary.active() == false) {
                logger.trace("primary shard [{}] is not yet active, scheduling a retry.", primary.shardId());
                retryBecauseUnavailable(shardIt.shardId(), "Primary shard is not active or isn't assigned to a known node.");
                return;
            }
            if (observer.observedState().nodes().nodeExists(primary.currentNodeId()) == false) {
                logger.trace("primary shard [{}] is assigned to anode we do not know the node, scheduling a retry.", primary.shardId(), primary.currentNodeId());
                retryBecauseUnavailable(shardIt.shardId(), "Primary shard is not active or isn't assigned to a known node.");
                return;
            }
            routeRequestOrPerformLocally(primary, shardIt);
        }

        /**
         * checks for any cluster state blocks. Returns true if operation is OK to proceeded.
         * if false is return, no further action is needed. The method takes care of any continuation, by either
         * responding to the listener or scheduling a retry
         */
        protected boolean checkBlocks() {
            ClusterBlockException blockException = checkGlobalBlock(observer.observedState());
            if (blockException != null) {
                if (blockException.retryable()) {
                    logger.trace("cluster is blocked ({}), scheduling a retry", blockException.getMessage());
                    retry(blockException);
                } else {
                    finishAsFailed(blockException);
                }
                return false;
            }
            if (resolveIndex()) {
                internalRequest.concreteIndex(indexNameExpressionResolver.concreteSingleIndex(observer.observedState(), internalRequest.request()));
            } else {
                internalRequest.concreteIndex(internalRequest.request().index());
            }

            resolveRequest(observer.observedState(), internalRequest, listener);

            blockException = checkRequestBlock(observer.observedState(), internalRequest);
            if (blockException != null) {
                if (blockException.retryable()) {
                    logger.trace("cluster is blocked ({}), scheduling a retry", blockException.getMessage());
                    retry(blockException);
                } else {
                    finishAsFailed(blockException);
                }
                return false;
            }
            return true;
        }

        protected ShardRouting resolvePrimary(ShardIterator shardIt) {
            // no shardIt, might be in the case between index gateway recovery and shardIt initialization
            ShardRouting shard;
            while ((shard = shardIt.nextOrNull()) != null) {
                // we only deal with primary shardIt here...
                if (shard.primary()) {
                    return shard;
                }
            }
            return null;
        }

        /**
         * send the request to the node holding the primary or execute if local
         */
        protected void routeRequestOrPerformLocally(final ShardRouting primary, final ShardIterator shardsIt) {
            if (primary.currentNodeId().equals(observer.observedState().nodes().localNodeId())) {
                try {
                    threadPool.executor(executor).execute(new AbstractRunnable() {
                        @Override
                        public void onFailure(Throwable t) {
                            finishAsFailed(t);
                        }

                        @Override
                        protected void doRun() throws Exception {
                            performOnPrimary(primary, shardsIt);
                        }
                    });
                } catch (Throwable t) {
                    finishAsFailed(t);
                }
            } else {
                DiscoveryNode node = observer.observedState().nodes().get(primary.currentNodeId());
                transportService.sendRequest(node, actionName, internalRequest.request(), transportOptions, new BaseTransportResponseHandler<Response>() {

                    @Override
                    public Response newInstance() {
                        return newResponseInstance();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(Response response) {
                        finishOnRemoteSuccess(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        try {
                            // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                            if (exp.unwrapCause() instanceof ConnectTransportException || exp.unwrapCause() instanceof NodeClosedException ||
                                    retryPrimaryException(exp)) {
                                internalRequest.request().setCanHaveDuplicates();
                                // we already marked it as started when we executed it (removed the listener) so pass false
                                // to re-add to the cluster listener
                                logger.trace("received an error from node the primary was assigned to ({}), scheduling a retry", exp.getMessage());
                                retry(exp);
                            } else {
                                finishAsFailed(exp);
                            }
                        } catch (Throwable t) {
                            finishWithUnexpectedFailure(t);
                        }
                    }
                });
            }
        }

        void retry(Throwable failure) {
            assert failure != null;
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                finishAsFailed(failure);
                return;
            }
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    run();
                }

                @Override
                public void onClusterServiceClose() {
                    finishAsFailed(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // Try one more time...
                    run();
                }
            });
        }

        /**
         * upon success, finish the first phase and transfer responsibility to the {@link ReplicationPhase}
         */
        void finishAndMoveToReplication(ReplicationPhase replicationPhase) {
            if (finished.compareAndSet(false, true)) {
                replicationPhase.run();
            } else {
                assert false : "finishAndMoveToReplication called but operation is already finished";
            }
        }


        void finishAsFailed(Throwable failure) {
            if (finished.compareAndSet(false, true)) {
                Releasables.close(indexShardReference);
                logger.trace("operation failed", failure);
                listener.onFailure(failure);
            } else {
                assert false : "finishAsFailed called but operation is already finished";
            }
        }

        void finishWithUnexpectedFailure(Throwable failure) {
            logger.warn("unexpected error during the primary phase for action [{}]", failure, actionName);
            if (finished.compareAndSet(false, true)) {
                Releasables.close(indexShardReference);
                listener.onFailure(failure);
            } else {
                assert false : "finishWithUnexpectedFailure called but operation is already finished";
            }
        }

        void finishOnRemoteSuccess(Response response) {
            if (finished.compareAndSet(false, true)) {
                logger.trace("operation succeeded");
                listener.onResponse(response);
            } else {
                assert false : "finishOnRemoteSuccess called but operation is already finished";
            }
        }

        /**
         * perform the operation on the node holding the primary
         */
        void performOnPrimary(final ShardRouting primary, final ShardIterator shardsIt) {
            final String writeConsistencyFailure = checkWriteConsistency(primary);
            if (writeConsistencyFailure != null) {
                retryBecauseUnavailable(primary.shardId(), writeConsistencyFailure);
                return;
            }
            final ReplicationPhase replicationPhase;
            try {
                indexShardReference = getIndexShardOperationsCounter(primary.shardId());
                PrimaryOperationRequest por = new PrimaryOperationRequest(primary.id(), internalRequest.concreteIndex(), internalRequest.request());
                Tuple<Response, ReplicaRequest> primaryResponse = shardOperationOnPrimary(observer.observedState(), por);
                logger.trace("operation completed on primary [{}]", primary);
                replicationPhase = new ReplicationPhase(shardsIt, primaryResponse.v2(), primaryResponse.v1(), observer, primary, internalRequest, listener, indexShardReference);
            } catch (Throwable e) {
                internalRequest.request.setCanHaveDuplicates();
                // shard has not been allocated yet, retry it here
                if (retryPrimaryException(e)) {
                    logger.trace("had an error while performing operation on primary ({}), scheduling a retry.", e.getMessage());
                    // We have to close here because when we retry we will increment get a new reference on index shard again and we do not want to
                    // increment twice.
                    Releasables.close(indexShardReference);
                    // We have to reset to null here because whe we retry it might be that we never get to the point where we assign a new reference
                    // (for example, in case the operation was rejected because queue is full). In this case we would release again once one of the finish methods is called.
                    indexShardReference = null;
                    retry(e);
                    return;
                }
                if (ExceptionsHelper.status(e) == RestStatus.CONFLICT) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(primary.shortSummary() + ": Failed to execute [" + internalRequest.request() + "]", e);
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(primary.shortSummary() + ": Failed to execute [" + internalRequest.request() + "]", e);
                    }
                }
                finishAsFailed(e);
                return;
            }
            finishAndMoveToReplication(replicationPhase);
        }

        /**
         * checks whether we can perform a write based on the write consistency setting
         * returns **null* if OK to proceed, or a string describing the reason to stop
         */
        String checkWriteConsistency(ShardRouting shard) {
            if (checkWriteConsistency == false) {
                return null;
            }

            final WriteConsistencyLevel consistencyLevel;
            if (internalRequest.request().consistencyLevel() != WriteConsistencyLevel.DEFAULT) {
                consistencyLevel = internalRequest.request().consistencyLevel();
            } else {
                consistencyLevel = defaultWriteConsistencyLevel;
            }
            final int sizeActive;
            final int requiredNumber;
            IndexRoutingTable indexRoutingTable = observer.observedState().getRoutingTable().index(shard.index());
            if (indexRoutingTable != null) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shard.getId());
                if (shardRoutingTable != null) {
                    sizeActive = shardRoutingTable.activeShards().size();
                    if (consistencyLevel == WriteConsistencyLevel.QUORUM && shardRoutingTable.getSize() > 2) {
                        // only for more than 2 in the number of shardIt it makes sense, otherwise its 1 shard with 1 replica, quorum is 1 (which is what it is initialized to)
                        requiredNumber = (shardRoutingTable.getSize() / 2) + 1;
                    } else if (consistencyLevel == WriteConsistencyLevel.ALL) {
                        requiredNumber = shardRoutingTable.getSize();
                    } else {
                        requiredNumber = 1;
                    }
                } else {
                    sizeActive = 0;
                    requiredNumber = 1;
                }
            } else {
                sizeActive = 0;
                requiredNumber = 1;
            }

            if (sizeActive < requiredNumber) {
                logger.trace("not enough active copies of shard [{}] to meet write consistency of [{}] (have {}, needed {}), scheduling a retry.",
                        shard.shardId(), consistencyLevel, sizeActive, requiredNumber);
                return "Not enough active copies to meet write consistency of [" + consistencyLevel + "] (have " + sizeActive + ", needed " + requiredNumber + ").";
            } else {
                return null;
            }
        }

        void retryBecauseUnavailable(ShardId shardId, String message) {
            retry(new UnavailableShardsException(shardId, message + " Timeout: [" + internalRequest.request().timeout() + "], request: " + internalRequest.request().toString()));
        }

    }

    protected Releasable getIndexShardOperationsCounter(ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.index().getName());
        IndexShard indexShard = indexService.shardSafe(shardId.id());
        return new IndexShardReference(indexShard);
    }

    private void failReplicaIfNeeded(String index, int shardId, Throwable t) {
        logger.trace("failure on replica [{}][{}]", t, index, shardId);
        if (ignoreReplicaException(t) == false) {
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                logger.debug("ignoring failed replica [{}][{}] because index was already removed.", index, shardId);
                return;
            }
            IndexShard indexShard = indexService.shard(shardId);
            if (indexShard == null) {
                logger.debug("ignoring failed replica [{}][{}] because index was already removed.", index, shardId);
                return;
            }
            indexShard.failShard(actionName + " failed on replica", t);
        }
    }

    /**
     * inner class is responsible for send the requests to all replica shards and manage the responses
     */
    final class ReplicationPhase extends AbstractRunnable {

        private final ReplicaRequest replicaRequest;
        private final Response finalResponse;
        private final ShardIterator shardIt;
        private final ActionListener<Response> listener;
        private final AtomicBoolean finished = new AtomicBoolean(false);
        private final AtomicInteger success = new AtomicInteger(1); // We already wrote into the primary shard
        private final ConcurrentMap<String, Throwable> shardReplicaFailures = ConcurrentCollections.newConcurrentMap();
        private final IndexMetaData indexMetaData;
        private final ShardRouting originalPrimaryShard;
        private final AtomicInteger pending;
        private final int totalShards;
        private final ClusterStateObserver observer;
        private final Releasable indexShardReference;

        /**
         * the constructor doesn't take any action, just calculates state. Call {@link #run()} to start
         * replicating.
         */
        public ReplicationPhase(ShardIterator originalShardIt, ReplicaRequest replicaRequest, Response finalResponse,
                                ClusterStateObserver observer, ShardRouting originalPrimaryShard,
                                InternalRequest internalRequest, ActionListener<Response> listener, Releasable indexShardReference) {
            this.replicaRequest = replicaRequest;
            this.listener = listener;
            this.finalResponse = finalResponse;
            this.originalPrimaryShard = originalPrimaryShard;
            this.observer = observer;
            indexMetaData = observer.observedState().metaData().index(internalRequest.concreteIndex());
            this.indexShardReference = indexShardReference;

            ShardRouting shard;
            // we double check on the state, if it got changed we need to make sure we take the latest one cause
            // maybe a replica shard started its recovery process and we need to apply it there...

            // we also need to make sure if the new state has a new primary shard (that we indexed to before) started
            // and assigned to another node (while the indexing happened). In that case, we want to apply it on the
            // new primary shard as well...
            ClusterState newState = clusterService.state();

            int numberOfUnassignedOrIgnoredReplicas = 0;
            int numberOfPendingShardInstances = 0;
            if (observer.observedState() != newState) {
                observer.reset(newState);
                shardIt = shards(newState, internalRequest);
                while ((shard = shardIt.nextOrNull()) != null) {
                    if (shard.primary()) {
                        if (originalPrimaryShard.currentNodeId().equals(shard.currentNodeId()) == false) {
                            // there is a new primary, we'll have to replicate to it.
                            numberOfPendingShardInstances++;
                        }
                        if (shard.relocating()) {
                            numberOfPendingShardInstances++;
                        }
                    } else if (shouldExecuteReplication(indexMetaData.settings()) == false) {
                        // If the replicas use shadow replicas, there is no reason to
                        // perform the action on the replica, so skip it and
                        // immediately return

                        // this delays mapping updates on replicas because they have
                        // to wait until they get the new mapping through the cluster
                        // state, which is why we recommend pre-defined mappings for
                        // indices using shadow replicas
                        numberOfUnassignedOrIgnoredReplicas++;
                    } else if (shard.unassigned()) {
                        numberOfUnassignedOrIgnoredReplicas++;
                    } else if (shard.relocating()) {
                        // we need to send to two copies
                        numberOfPendingShardInstances += 2;
                    } else {
                        numberOfPendingShardInstances++;
                    }
                }
                internalRequest.request().setCanHaveDuplicates(); // safe side, cluster state changed, we might have dups
            } else {
                shardIt = originalShardIt;
                shardIt.reset();
                while ((shard = shardIt.nextOrNull()) != null) {
                    if (shard.state() != ShardRoutingState.STARTED) {
                        replicaRequest.setCanHaveDuplicates();
                    }
                    if (shard.unassigned()) {
                        numberOfUnassignedOrIgnoredReplicas++;
                    } else if (shard.primary()) {
                        if (shard.relocating()) {
                            // we have to replicate to the other copy
                            numberOfPendingShardInstances += 1;
                        }
                    } else if (shouldExecuteReplication(indexMetaData.settings()) == false) {
                        // If the replicas use shadow replicas, there is no reason to
                        // perform the action on the replica, so skip it and
                        // immediately return

                        // this delays mapping updates on replicas because they have
                        // to wait until they get the new mapping through the cluster
                        // state, which is why we recommend pre-defined mappings for
                        // indices using shadow replicas
                        numberOfUnassignedOrIgnoredReplicas++;
                    } else if (shard.relocating()) {
                        // we need to send to two copies
                        numberOfPendingShardInstances += 2;
                    } else {
                        numberOfPendingShardInstances++;
                    }
                }
            }

            // one for the primary already done
            this.totalShards = 1 + numberOfPendingShardInstances + numberOfUnassignedOrIgnoredReplicas;
            this.pending = new AtomicInteger(numberOfPendingShardInstances);
        }

        /**
         * total shard copies
         */
        int totalShards() {
            return totalShards;
        }

        /**
         * total successful operations so far
         */
        int successful() {
            return success.get();
        }

        /**
         * number of pending operations
         */
        int pending() {
            return pending.get();
        }

        @Override
        public void onFailure(Throwable t) {
            logger.error("unexpected error while replicating for action [{}]. shard [{}]. ", t, actionName, shardIt.shardId());
            forceFinishAsFailed(t);
        }

        /**
         * start sending current requests to replicas
         */
        @Override
        protected void doRun() {
            if (pending.get() == 0) {
                doFinish();
                return;
            }
            ShardRouting shard;
            shardIt.reset(); // reset the iterator
            while ((shard = shardIt.nextOrNull()) != null) {
                // if its unassigned, nothing to do here...
                if (shard.unassigned()) {
                    continue;
                }

                // we index on a replica that is initializing as well since we might not have got the event
                // yet that it was started. We will get an exception IllegalShardState exception if its not started
                // and that's fine, we will ignore it
                if (shard.primary()) {
                    if (originalPrimaryShard.currentNodeId().equals(shard.currentNodeId()) == false) {
                        // there is a new primary, we'll have to replicate to it.
                        performOnReplica(shard, shard.currentNodeId());
                    }
                    if (shard.relocating()) {
                        performOnReplica(shard, shard.relocatingNodeId());
                    }
                } else if (shouldExecuteReplication(indexMetaData.settings())) {
                    performOnReplica(shard, shard.currentNodeId());
                    if (shard.relocating()) {
                        performOnReplica(shard, shard.relocatingNodeId());
                    }
                }
            }
        }

        /**
         * send operation to the given node or perform it if local
         */
        void performOnReplica(final ShardRouting shard, final String nodeId) {
            // if we don't have that node, it means that it might have failed and will be created again, in
            // this case, we don't have to do the operation, and just let it failover
            if (!observer.observedState().nodes().nodeExists(nodeId)) {
                onReplicaFailure(nodeId, null);
                return;
            }

            replicaRequest.internalShardId = shardIt.shardId();

            if (!nodeId.equals(observer.observedState().nodes().localNodeId())) {
                final DiscoveryNode node = observer.observedState().nodes().get(nodeId);
                transportService.sendRequest(node, transportReplicaAction, replicaRequest,
                        transportOptions, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                            @Override
                            public void handleResponse(TransportResponse.Empty vResponse) {
                                onReplicaSuccess();
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                onReplicaFailure(nodeId, exp);
                                logger.trace("[{}] transport failure during replica request [{}] ", exp, node, replicaRequest);
                                if (ignoreReplicaException(exp) == false) {
                                    logger.warn("{} failed to perform {} on node {}", exp, shardIt.shardId(), actionName, node);
                                    shardStateAction.shardFailed(shard, indexMetaData.getIndexUUID(), "failed to perform " + actionName + " on replica on node " + node, exp);
                                }
                            }

                        });
            } else {
                try {
                    threadPool.executor(executor).execute(new AbstractRunnable() {
                        @Override
                        protected void doRun() {
                            try {
                                shardOperationOnReplica(shard.shardId(), replicaRequest);
                                onReplicaSuccess();
                            } catch (Throwable e) {
                                onReplicaFailure(nodeId, e);
                                failReplicaIfNeeded(shard.index(), shard.id(), e);
                            }
                        }

                        // we must never reject on because of thread pool capacity on replicas
                        @Override
                        public boolean isForceExecution() {
                            return true;
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            onReplicaFailure(nodeId, t);
                        }
                    });
                } catch (Throwable e) {
                    failReplicaIfNeeded(shard.index(), shard.id(), e);
                    onReplicaFailure(nodeId, e);
                }
            }
        }


        void onReplicaFailure(String nodeId, @Nullable Throwable e) {
            // Only version conflict should be ignored from being put into the _shards header?
            if (e != null && ignoreReplicaException(e) == false) {
                shardReplicaFailures.put(nodeId, e);
            }
            decPendingAndFinishIfNeeded();
        }

        void onReplicaSuccess() {
            success.incrementAndGet();
            decPendingAndFinishIfNeeded();
        }

        private void decPendingAndFinishIfNeeded() {
            if (pending.decrementAndGet() <= 0) {
                doFinish();
            }
        }

        private void forceFinishAsFailed(Throwable t) {
            if (finished.compareAndSet(false, true)) {
                Releasables.close(indexShardReference);
                listener.onFailure(t);
            }
        }

        private void doFinish() {
            if (finished.compareAndSet(false, true)) {
                Releasables.close(indexShardReference);
                final ShardId shardId = shardIt.shardId();
                final ActionWriteResponse.ShardInfo.Failure[] failuresArray;
                if (!shardReplicaFailures.isEmpty()) {
                    int slot = 0;
                    failuresArray = new ActionWriteResponse.ShardInfo.Failure[shardReplicaFailures.size()];
                    for (Map.Entry<String, Throwable> entry : shardReplicaFailures.entrySet()) {
                        RestStatus restStatus = ExceptionsHelper.status(entry.getValue());
                        failuresArray[slot++] = new ActionWriteResponse.ShardInfo.Failure(
                                shardId.getIndex(), shardId.getId(), entry.getKey(), entry.getValue(), restStatus, false
                        );
                    }
                } else {
                    failuresArray = ActionWriteResponse.EMPTY;
                }
                finalResponse.setShardInfo(new ActionWriteResponse.ShardInfo(
                                totalShards,
                                success.get(),
                                failuresArray

                        )
                );
                listener.onResponse(finalResponse);
            }
        }

    }

    /**
     * Indicated whether this operation should be replicated to shadow replicas or not. If this method returns true the replication phase will be skipped.
     * For example writes such as index and delete don't need to be replicated on shadow replicas but refresh and flush do.
     */
    protected boolean shouldExecuteReplication(Settings settings) {
        return IndexMetaData.isIndexUsingShadowReplicas(settings) == false;
    }

    /**
     * Internal request class that gets built on each node. Holds the original request plus additional info.
     */
    protected class InternalRequest {
        final Request request;
        String concreteIndex;

        InternalRequest(Request request) {
            this.request = request;
        }

        public Request request() {
            return request;
        }

        void concreteIndex(String concreteIndex) {
            this.concreteIndex = concreteIndex;
        }

        public String concreteIndex() {
            return concreteIndex;
        }
    }

    static class IndexShardReference implements Releasable {

        final private IndexShard counter;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        IndexShardReference(IndexShard counter) {
            counter.incrementOperationCounter();
            this.counter = counter;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                counter.decrementOperationCounter();
            }
        }
    }

    /** Utility method to create either an index or a create operation depending
     *  on the {@link OpType} of the request. */
    private final Engine.IndexingOperation prepareIndexOperationOnPrimary(BulkShardRequest shardRequest, IndexRequest request, IndexShard indexShard) {
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, request.source()).index(request.index()).type(request.type()).id(request.id())
                .routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());
        boolean canHaveDuplicates = request.canHaveDuplicates();
        if (shardRequest != null) {
            canHaveDuplicates |= shardRequest.canHaveDuplicates();
        }
        if (request.opType() == IndexRequest.OpType.INDEX) {
            return indexShard.prepareIndex(sourceToParse, request.version(), request.versionType(), Engine.Operation.Origin.PRIMARY, canHaveDuplicates);
        } else {
            assert request.opType() == IndexRequest.OpType.CREATE : request.opType();
            return indexShard.prepareCreate(sourceToParse,
                    request.version(), request.versionType(), Engine.Operation.Origin.PRIMARY, canHaveDuplicates, canHaveDuplicates);
        }
    }

    /** Execute the given {@link IndexRequest} on a primary shard, throwing a
     *  {@link RetryOnPrimaryException} if the operation needs to be re-tried. */
    protected final WriteResult<IndexResponse> executeIndexRequestOnPrimary(BulkShardRequest shardRequest, IndexRequest request, IndexShard indexShard) throws Throwable {
        Engine.IndexingOperation operation = prepareIndexOperationOnPrimary(shardRequest, request, indexShard);
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        final ShardId shardId = indexShard.shardId();
        if (update != null) {
            final String indexName = shardId.getIndex();
            mappingUpdatedAction.updateMappingOnMasterSynchronously(indexName, request.type(), update);
            operation = prepareIndexOperationOnPrimary(shardRequest, request, indexShard);
            update = operation.parsedDoc().dynamicMappingsUpdate();
            if (update != null) {
                throw new RetryOnPrimaryException(shardId,
                        "Dynamics mappings are not available on the node that holds the primary yet");
            }
        }
        final boolean created = operation.execute(indexShard);

        // update the version on request so it will happen on the replicas
        final long version = operation.version();
        request.version(version);
        request.versionType(request.versionType().versionTypeForReplicationAndRecovery());

        assert request.versionType().validateVersionForWrites(request.version());

        return new WriteResult(new IndexResponse(shardId.getIndex(), request.type(), request.id(), request.version(), created), operation.getTranslogLocation());
    }
}
