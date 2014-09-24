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

import org.elasticsearch.*;
import org.elasticsearch.action.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public abstract class TransportShardReplicationOperationAction<Request extends ShardReplicationOperationRequest, ReplicaRequest extends ShardReplicationOperationRequest, Response extends ActionResponse> extends TransportAction<Request, Response> {

    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final IndicesService indicesService;
    protected final ShardStateAction shardStateAction;
    protected final ReplicationType defaultReplicationType;
    protected final WriteConsistencyLevel defaultWriteConsistencyLevel;
    protected final TransportRequestOptions transportOptions;

    final String transportReplicaAction;
    final String executor;
    final boolean checkWriteConsistency;

    protected TransportShardReplicationOperationAction(Settings settings, String actionName, TransportService transportService,
                                                       ClusterService clusterService, IndicesService indicesService,
                                                       ThreadPool threadPool, ShardStateAction shardStateAction, ActionFilters actionFilters) {
        super(settings, actionName, threadPool, actionFilters);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardStateAction = shardStateAction;

        this.transportReplicaAction = actionName + "[r]";
        this.executor = executor();
        this.checkWriteConsistency = checkWriteConsistency();

        transportService.registerHandler(actionName, new OperationTransportHandler());
        transportService.registerHandler(transportReplicaAction, new ReplicaOperationTransportHandler());

        this.transportOptions = transportOptions();

        this.defaultReplicationType = ReplicationType.fromString(settings.get("action.replication_type", "sync"));
        this.defaultWriteConsistencyLevel = WriteConsistencyLevel.fromString(settings.get("action.write_consistency", "quorum"));
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncShardOperationAction(request, listener).start();
    }

    protected abstract Request newRequestInstance();

    protected abstract ReplicaRequest newReplicaRequestInstance();

    protected abstract Response newResponseInstance();

    protected abstract String executor();

    protected abstract PrimaryResponse<Response, ReplicaRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest);

    protected abstract void shardOperationOnReplica(ReplicaOperationRequest shardRequest);

    /**
     * Called once replica operations have been dispatched on the
     */
    protected void postPrimaryOperation(InternalRequest request, PrimaryResponse<Response, ReplicaRequest> response) {

    }

    protected abstract ShardIterator shards(ClusterState clusterState, InternalRequest request) throws ElasticsearchException;

    protected abstract boolean checkWriteConsistency();

    protected ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.concreteIndex());
    }

    protected abstract boolean resolveIndex();

    /**
     * Resolves the request, by default doing nothing. If the resolve
     * means a different execution, then return false here to indicate not to continue and execute this request.
     */
    protected boolean resolveRequest(ClusterState state, InternalRequest request, ActionListener<Response> listener) {
        return true;
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    /**
     * Should the operations be performed on the replicas as well. Defaults to <tt>false</tt> meaning operations
     * will be executed on the replica.
     */
    protected boolean ignoreReplicas() {
        return false;
    }

    protected boolean retryPrimaryException(Throwable e) {
        return TransportActions.isShardNotAvailableException(e);
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

    class OperationTransportHandler extends BaseTransportRequestHandler<Request> {

        @Override
        public Request newInstance() {
            return newRequestInstance();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
            // if we have a local operation, execute it on a thread since we don't spawn
            request.operationThreaded(true);
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

    class ReplicaOperationTransportHandler extends BaseTransportRequestHandler<ReplicaOperationRequest> {

        @Override
        public ReplicaOperationRequest newInstance() {
            return new ReplicaOperationRequest();
        }

        @Override
        public String executor() {
            return executor;
        }

        // we must never reject on because of thread pool capacity on replicas
        @Override
        public boolean isForceExecution() {
            return true;
        }

        @Override
        public void messageReceived(final ReplicaOperationRequest request, final TransportChannel channel) throws Exception {
            try {
                shardOperationOnReplica(request);
            } catch (Throwable t) {
                failReplicaIfNeeded(request.shardId.getIndex(), request.shardId.id(), t);
                throw t;
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    protected class PrimaryOperationRequest {
        public ShardId shardId;
        public Request request;

        public PrimaryOperationRequest(int shardId, String index, Request request) {
            this.shardId = new ShardId(index, shardId);
            this.request = request;
        }
    }

    protected class ReplicaOperationRequest extends TransportRequest implements IndicesRequest {

        public ShardId shardId;
        public ReplicaRequest request;

        ReplicaOperationRequest() {
        }

        ReplicaOperationRequest(ShardId shardId, ReplicaRequest request) {
            super(request);
            this.shardId = shardId;
            this.request = request;
        }

        @Override
        public String[] indices() {
            return request.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            return request.indicesOptions();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            int shard = -1;
            if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
                shardId = ShardId.readShardId(in);
            } else {
                shard = in.readVInt();
            }
            request = newReplicaRequestInstance();
            request.readFrom(in);
            if (in.getVersion().before(Version.V_1_4_0_Beta1)) {
                assert shard >= 0;
                //older nodes will send the concrete index as part of the request
                shardId = new ShardId(request.index(), shard);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
                shardId.writeTo(out);
            } else {
                out.writeVInt(shardId.id());
                //older nodes expect the concrete index as part of the request
                request.index(shardId.getIndex());
            }
            request.writeTo(out);
        }
    }

    protected class AsyncShardOperationAction {
        private final ActionListener<Response> listener;
        private final InternalRequest internalRequest;
        private volatile ShardIterator shardIt;
        private final AtomicBoolean primaryOperationStarted = new AtomicBoolean();
        private final ReplicationType replicationType;
        private volatile ClusterStateObserver observer;

        AsyncShardOperationAction(Request request, ActionListener<Response> listener) {
            this.internalRequest = new InternalRequest(request);
            this.listener = listener;

            if (request.replicationType() != ReplicationType.DEFAULT) {
                replicationType = request.replicationType();
            } else {
                replicationType = defaultReplicationType;
            }
        }

        public void start() {
            this.observer = new ClusterStateObserver(clusterService, internalRequest.request().timeout(), logger);
            doStart();
        }

        /**
         * Returns <tt>true</tt> if the action starting to be performed on the primary (or is done).
         */
        protected void doStart() throws ElasticsearchException {
            try {
                ClusterBlockException blockException = checkGlobalBlock(observer.observedState());
                if (blockException != null) {
                    if (blockException.retryable()) {
                        logger.trace("cluster is blocked ({}), scheduling a retry", blockException.getMessage());
                        retry(blockException);
                        return;
                    } else {
                        throw blockException;
                    }
                }
                if (resolveIndex()) {
                    internalRequest.concreteIndex(observer.observedState().metaData().concreteSingleIndex(internalRequest.request().index(), internalRequest.request().indicesOptions()));
                } else {
                    internalRequest.concreteIndex(internalRequest.request().index());
                }
                // check if we need to execute, and if not, return
                if (!resolveRequest(observer.observedState(), internalRequest, listener)) {
                    return;
                }
                blockException = checkRequestBlock(observer.observedState(), internalRequest);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        logger.trace("cluster is blocked ({}), scheduling a retry", blockException.getMessage());
                        retry(blockException);
                        return;
                    } else {
                        throw blockException;
                    }
                }
                shardIt = shards(observer.observedState(), internalRequest);
            } catch (Throwable e) {
                listener.onFailure(e);
                return;
            }

            // no shardIt, might be in the case between index gateway recovery and shardIt initialization
            if (shardIt.size() == 0) {
                logger.trace("no shard instances known for shard [{}], scheduling a retry", shardIt.shardId());
                retryBecauseUnavailable(shardIt.shardId(), "No active shards.");
                return;
            }

            boolean foundPrimary = false;
            ShardRouting shardX;
            while ((shardX = shardIt.nextOrNull()) != null) {
                final ShardRouting shard = shardX;
                // we only deal with primary shardIt here...
                if (!shard.primary()) {
                    continue;
                }
                if (!shard.active() || !observer.observedState().nodes().nodeExists(shard.currentNodeId())) {
                    logger.trace("primary shard [{}] is not yet active or we do not know the node it is assigned to [{}], scheduling a retry.", shard.shardId(), shard.currentNodeId());
                    retryBecauseUnavailable(shardIt.shardId(), "Primary shard is not active or isn't assigned is a known node.");
                    return;
                }

                if (!primaryOperationStarted.compareAndSet(false, true)) {
                    return;
                }

                foundPrimary = true;
                if (shard.currentNodeId().equals(observer.observedState().nodes().localNodeId())) {
                    try {
                        if (internalRequest.request().operationThreaded()) {
                            internalRequest.request().beforeLocalFork();
                            threadPool.executor(executor).execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        performOnPrimary(shard.id(), shard);
                                    } catch (Throwable t) {
                                        listener.onFailure(t);
                                    }
                                }
                            });
                        } else {
                            performOnPrimary(shard.id(), shard);
                        }
                    } catch (Throwable t) {
                        listener.onFailure(t);
                    }
                } else {
                    DiscoveryNode node = observer.observedState().nodes().get(shard.currentNodeId());
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
                            listener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                            if (exp.unwrapCause() instanceof ConnectTransportException || exp.unwrapCause() instanceof NodeClosedException ||
                                    retryPrimaryException(exp)) {
                                primaryOperationStarted.set(false);
                                // we already marked it as started when we executed it (removed the listener) so pass false
                                // to re-add to the cluster listener
                                logger.trace("received an error from node the primary was assigned to ({}), scheduling a retry", exp.getMessage());
                                retry(exp);
                            } else {
                                listener.onFailure(exp);
                            }
                        }
                    });
                }
                break;
            }
            // we won't find a primary if there are no shards in the shard iterator, retry...
            if (!foundPrimary) {
                logger.trace("couldn't find a eligible primary shard, scheduling for retry.");
                retryBecauseUnavailable(shardIt.shardId(), "No active shards.");
            }
        }

        void retry(Throwable failure) {
            assert failure != null;
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                listener.onFailure(failure);
                return;
            }
            // make it threaded operation so we fork on the discovery listener thread
            internalRequest.request().beforeLocalFork();
            internalRequest.request().operationThreaded(true);

            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    doStart();
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // Try one more time...
                    doStart();
                }
            });
        }

        void performOnPrimary(int primaryShardId, final ShardRouting shard) {
            ClusterState clusterState = observer.observedState();
            if (raiseFailureIfHaveNotEnoughActiveShardCopies(shard, clusterState)) {
                return;
            }
            try {
                PrimaryResponse<Response, ReplicaRequest> response = shardOperationOnPrimary(clusterState, new PrimaryOperationRequest(primaryShardId, internalRequest.concreteIndex(), internalRequest.request()));
                performReplicas(response);
            } catch (Throwable e) {
                internalRequest.request.setCanHaveDuplicates();
                // shard has not been allocated yet, retry it here
                if (retryPrimaryException(e)) {
                    primaryOperationStarted.set(false);
                    logger.trace("had an error while performing operation on primary ({}), scheduling a retry.", e.getMessage());
                    retry(e);
                    return;
                }
                if (e instanceof ElasticsearchException && ((ElasticsearchException) e).status() == RestStatus.CONFLICT) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(shard.shortSummary() + ": Failed to execute [" + internalRequest.request() + "]", e);
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(shard.shortSummary() + ": Failed to execute [" + internalRequest.request() + "]", e);
                    }
                }
                listener.onFailure(e);
            }
        }

        void performReplicas(final PrimaryResponse<Response, ReplicaRequest> response) {
            if (ignoreReplicas()) {
                postPrimaryOperation(internalRequest, response);
                listener.onResponse(response.response());
                return;
            }
            ShardRouting shard;

            // we double check on the state, if it got changed we need to make sure we take the latest one cause
            // maybe a replica shard started its recovery process and we need to apply it there...

            // we also need to make sure if the new state has a new primary shard (that we indexed to before) started
            // and assigned to another node (while the indexing happened). In that case, we want to apply it on the
            // new primary shard as well...
            ClusterState newState = clusterService.state();
            ShardRouting newPrimaryShard = null;
            if (observer.observedState() != newState) {
                shardIt.reset();
                ShardRouting originalPrimaryShard = null;
                while ((shard = shardIt.nextOrNull()) != null) {
                    if (shard.primary()) {
                        originalPrimaryShard = shard;
                        break;
                    }
                }
                if (originalPrimaryShard == null || !originalPrimaryShard.active()) {
                    throw new ElasticsearchIllegalStateException("unexpected state, failed to find primary shard on an index operation that succeeded");
                }

                observer.reset(newState);
                shardIt = shards(newState, internalRequest);
                while ((shard = shardIt.nextOrNull()) != null) {
                    if (shard.primary()) {
                        if (originalPrimaryShard.currentNodeId().equals(shard.currentNodeId())) {
                            newPrimaryShard = null;
                        } else {
                            newPrimaryShard = shard;
                        }
                        break;
                    }
                }
                shardIt.reset();
                internalRequest.request().setCanHaveDuplicates(); // safe side, cluster state changed, we might have dups
            } else {
                shardIt.reset();
                while ((shard = shardIt.nextOrNull()) != null) {
                    if (shard.state() != ShardRoutingState.STARTED) {
                        internalRequest.request().setCanHaveDuplicates();
                    }
                }
                shardIt.reset();
            }

            // initialize the counter
            int replicaCounter = shardIt.assignedReplicasIncludingRelocating();

            if (newPrimaryShard != null) {
                replicaCounter++;
            }

            if (replicaCounter == 0) {
                postPrimaryOperation(internalRequest, response);
                listener.onResponse(response.response());
                return;
            }

            if (replicationType == ReplicationType.ASYNC) {
                postPrimaryOperation(internalRequest, response);
                // async replication, notify the listener
                listener.onResponse(response.response());
                // now, trick the counter so it won't decrease to 0 and notify the listeners
                replicaCounter = Integer.MIN_VALUE;
            }

            // we add one to the replica count to do the postPrimaryOperation
            replicaCounter++;
            AtomicInteger counter = new AtomicInteger(replicaCounter);


            IndexMetaData indexMetaData = observer.observedState().metaData().index(internalRequest.concreteIndex());

            if (newPrimaryShard != null) {
                performOnReplica(response, counter, newPrimaryShard, newPrimaryShard.currentNodeId(), indexMetaData);
            }

            shardIt.reset(); // reset the iterator
            while ((shard = shardIt.nextOrNull()) != null) {
                // if its unassigned, nothing to do here...
                if (shard.unassigned()) {
                    continue;
                }

                // if the shard is primary and relocating, add one to the counter since we perform it on the replica as well
                // (and we already did it on the primary)
                boolean doOnlyOnRelocating = false;
                if (shard.primary()) {
                    if (shard.relocating()) {
                        doOnlyOnRelocating = true;
                    } else {
                        continue;
                    }
                }
                // we index on a replica that is initializing as well since we might not have got the event
                // yet that it was started. We will get an exception IllegalShardState exception if its not started
                // and that's fine, we will ignore it
                if (!doOnlyOnRelocating) {
                    performOnReplica(response, counter, shard, shard.currentNodeId(), indexMetaData);
                }
                if (shard.relocating()) {
                    performOnReplica(response, counter, shard, shard.relocatingNodeId(), indexMetaData);
                }
            }

            // now do the postPrimary operation, and check if the listener needs to be invoked
            postPrimaryOperation(internalRequest, response);
            // we also invoke here in case replicas finish before postPrimaryAction does
            if (counter.decrementAndGet() == 0) {
                listener.onResponse(response.response());
            }
        }

        void performOnReplica(final PrimaryResponse<Response, ReplicaRequest> response, final AtomicInteger counter, final ShardRouting shard, String nodeId, final IndexMetaData indexMetaData) {
            // if we don't have that node, it means that it might have failed and will be created again, in
            // this case, we don't have to do the operation, and just let it failover
            if (!observer.observedState().nodes().nodeExists(nodeId)) {
                if (counter.decrementAndGet() == 0) {
                    listener.onResponse(response.response());
                }
                return;
            }

            final ReplicaOperationRequest shardRequest = new ReplicaOperationRequest(shardIt.shardId(), response.replicaRequest());
            if (!nodeId.equals(observer.observedState().nodes().localNodeId())) {
                final DiscoveryNode node = observer.observedState().nodes().get(nodeId);
                transportService.sendRequest(node, transportReplicaAction, shardRequest, transportOptions, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty vResponse) {
                        finishIfPossible();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.trace("[{}] Transport failure during replica request [{}] ", exp, node, internalRequest.request());
                        if (!ignoreReplicaException(exp)) {
                            logger.warn("Failed to perform " + actionName + " on remote replica " + node + shardIt.shardId(), exp);
                            shardStateAction.shardFailed(shard, indexMetaData.getUUID(),
                                    "Failed to perform [" + actionName + "] on replica, message [" + ExceptionsHelper.detailedMessage(exp) + "]");
                        }
                        finishIfPossible();
                    }

                    private void finishIfPossible() {
                        if (counter.decrementAndGet() == 0) {
                            listener.onResponse(response.response());
                        }
                    }
                });
            } else {
                if (internalRequest.request().operationThreaded()) {
                    internalRequest.request().beforeLocalFork();
                    try {
                        threadPool.executor(executor).execute(new AbstractRunnable() {
                            @Override
                            public void run() {
                                try {
                                    shardOperationOnReplica(shardRequest);
                                } catch (Throwable e) {
                                    failReplicaIfNeeded(shard.index(), shard.id(), e);
                                }
                                if (counter.decrementAndGet() == 0) {
                                    listener.onResponse(response.response());
                                }
                            }

                            // we must never reject on because of thread pool capacity on replicas
                            @Override
                            public boolean isForceExecution() {
                                return true;
                            }
                        });
                    } catch (Throwable e) {
                        failReplicaIfNeeded(shard.index(), shard.id(), e);
                        // we want to decrement the counter here, in teh failure handling, cause we got rejected
                        // from executing on the thread pool
                        if (counter.decrementAndGet() == 0) {
                            listener.onResponse(response.response());
                        }
                    }
                } else {
                    try {
                        shardOperationOnReplica(shardRequest);
                    } catch (Throwable e) {
                        failReplicaIfNeeded(shard.index(), shard.id(), e);
                    }
                    if (counter.decrementAndGet() == 0) {
                        listener.onResponse(response.response());
                    }
                }
            }
        }

        boolean raiseFailureIfHaveNotEnoughActiveShardCopies(ShardRouting shard, ClusterState state) {
            if (!checkWriteConsistency) {
                return false;
            }

            final WriteConsistencyLevel consistencyLevel;
            if (internalRequest.request().consistencyLevel() != WriteConsistencyLevel.DEFAULT) {
                consistencyLevel = internalRequest.request().consistencyLevel();
            } else {
                consistencyLevel = defaultWriteConsistencyLevel;
            }
            final int sizeActive;
            final int requiredNumber;
            IndexRoutingTable indexRoutingTable =  state.getRoutingTable().index(shard.index());
            if (indexRoutingTable != null) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shard.getId());
                if (shardRoutingTable != null) {
                    sizeActive =  shardRoutingTable.activeShards().size();
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
                primaryOperationStarted.set(false);
                // A dedicated exception would be nice...
                retryBecauseUnavailable(shard.shardId(), "Not enough active copies to meet write consistency of [" + consistencyLevel + "] (have " + sizeActive + ", needed " + requiredNumber + ").");
                return true;
            } else {
                return false;
            }
        }

        void retryBecauseUnavailable(ShardId shardId, String message) {
            retry(new UnavailableShardsException(shardId, message + " Timeout: [" + internalRequest.request().timeout() +"], request: " +  internalRequest.request().toString()));
        }

    }

    private void failReplicaIfNeeded(String index, int shardId, Throwable t) {
        logger.trace("failure on replica [{}][{}]", t, index, shardId);
        if (!ignoreReplicaException(t)) {
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

    public static class PrimaryResponse<Response, ReplicaRequest> {
        private final ReplicaRequest replicaRequest;
        private final Response response;
        private final Object payload;

        public PrimaryResponse(ReplicaRequest replicaRequest, Response response, Object payload) {
            this.replicaRequest = replicaRequest;
            this.response = response;
            this.payload = payload;
        }

        public ReplicaRequest replicaRequest() {
            return this.replicaRequest;
        }

        public Response response() {
            return response;
        }

        public Object payload() {
            return payload;
        }
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
}
