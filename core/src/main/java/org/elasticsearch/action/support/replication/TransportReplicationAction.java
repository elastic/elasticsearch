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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportChannelResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Base class for requests that should be executed on a primary copy followed by replica copies.
 * Subclasses can resolve the target shard and provide implementation for primary and replica operations.
 *
 * The action samples cluster state on the receiving node to reroute to node with primary copy and on the
 * primary node to validate request before primary operation followed by sampling state again for resolving
 * nodes with replica copies to perform replication.
 */
public abstract class TransportReplicationAction<
            Request extends ReplicationRequest<Request>,
            ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
            Response extends ReplicationResponse
        > extends TransportAction<Request, Response> {

    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final IndicesService indicesService;
    private final ShardStateAction shardStateAction;
    private final TransportRequestOptions transportOptions;
    private final String executor;

    // package private for testing
    final String transportReplicaAction;
    final String transportPrimaryAction;
    private final ReplicasProxy replicasProxy;

    protected TransportReplicationAction(Settings settings, String actionName, TransportService transportService,
                                         ClusterService clusterService, IndicesService indicesService,
                                         ThreadPool threadPool, ShardStateAction shardStateAction,
                                         ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request,
                                         Supplier<ReplicaRequest> replicaRequest, String executor) {
        super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardStateAction = shardStateAction;
        this.executor = executor;

        this.transportPrimaryAction = actionName + "[p]";
        this.transportReplicaAction = actionName + "[r]";
        transportService.registerRequestHandler(actionName, request, ThreadPool.Names.SAME, new OperationTransportHandler());
        transportService.registerRequestHandler(transportPrimaryAction, request, executor, new PrimaryOperationTransportHandler());
        // we must never reject on because of thread pool capacity on replicas
        transportService.registerRequestHandler(transportReplicaAction, replicaRequest, executor, true, true,
            new ReplicaOperationTransportHandler());

        this.transportOptions = transportOptions();

        this.replicasProxy = new ReplicasProxy();
    }

    @Override
    protected final void doExecute(Request request, ActionListener<Response> listener) {
        throw new UnsupportedOperationException("the task parameter is required for this operation");
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new ReroutePhase((ReplicationTask) task, request, listener).run();
    }

    protected abstract Response newResponseInstance();

    /**
     * Resolves derived values in the request. For example, the target shard id of the incoming request,
     * if not set at request construction
     * Additional processing or validation of the request should be done here.
     *
     * @param metaData      cluster state metadata
     * @param indexMetaData index metadata of the concrete index this request is going to operate on
     * @param request       the request to resolve
     */
    protected void resolveRequest(MetaData metaData, IndexMetaData indexMetaData, Request request) {
        if (request.waitForActiveShards() == ActiveShardCount.DEFAULT) {
            // if the wait for active shard count has not been set in the request,
            // resolve it from the index settings
            request.waitForActiveShards(indexMetaData.getWaitForActiveShards());
        }
    }

    /**
     * Primary operation on node with primary copy.
     *
     * @param shardRequest the request to the primary shard
     */
    protected abstract PrimaryResult shardOperationOnPrimary(Request shardRequest) throws Exception;

    /**
     * Synchronous replica operation on nodes with replica copies. This is done under the lock form
     * {@link #acquireReplicaOperationLock(ShardId, long, ActionListener)}.
     */
    protected abstract ReplicaResult shardOperationOnReplica(ReplicaRequest shardRequest);

    /**
     * Cluster level block to check before request execution
     */
    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    /**
     * Index level block to check before request execution
     */
    protected ClusterBlockLevel indexBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    /**
     * True if provided index should be resolved when resolving request
     */
    protected boolean resolveIndex() {
        return true;
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    protected boolean retryPrimaryException(final Throwable e) {
        return e.getClass() == ReplicationOperation.RetryOnPrimaryException.class
            || TransportActions.isShardNotAvailableException(e);
    }

    class OperationTransportHandler implements TransportRequestHandler<Request> {
        @Override
        public void messageReceived(final Request request, final TransportChannel channel, Task task) throws Exception {
            execute(task, request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn("Failed to send response for {}", inner, actionName);
                    }
                }
            });
        }

        @Override
        public void messageReceived(Request request, TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }
    }

    class PrimaryOperationTransportHandler implements TransportRequestHandler<Request> {
        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }

        @Override
        public void messageReceived(Request request, TransportChannel channel, Task task) {
            new AsyncPrimaryAction(request, channel, (ReplicationTask) task).run();
        }
    }

    class AsyncPrimaryAction extends AbstractRunnable implements ActionListener<PrimaryShardReference> {

        private final Request request;
        private final TransportChannel channel;
        private final ReplicationTask replicationTask;

        AsyncPrimaryAction(Request request, TransportChannel channel, ReplicationTask replicationTask) {
            this.request = request;
            this.channel = channel;
            this.replicationTask = replicationTask;
        }

        @Override
        protected void doRun() throws Exception {
            acquirePrimaryShardReference(request.shardId(), this);
        }

        @Override
        public void onResponse(PrimaryShardReference primaryShardReference) {
            try {
                if (primaryShardReference.isRelocated()) {
                    primaryShardReference.close(); // release shard operation lock as soon as possible
                    setPhase(replicationTask, "primary_delegation");
                    // delegate primary phase to relocation target
                    // it is safe to execute primary phase on relocation target as there are no more in-flight operations where primary
                    // phase is executed on local shard and all subsequent operations are executed on relocation target as primary phase.
                    final ShardRouting primary = primaryShardReference.routingEntry();
                    assert primary.relocating() : "indexShard is marked as relocated but routing isn't" + primary;
                    DiscoveryNode relocatingNode = clusterService.state().nodes().get(primary.relocatingNodeId());
                    transportService.sendRequest(relocatingNode, transportPrimaryAction, request, transportOptions,
                        new TransportChannelResponseHandler<Response>(logger, channel, "rerouting indexing to target primary " + primary,
                            TransportReplicationAction.this::newResponseInstance) {

                            @Override
                            public void handleResponse(Response response) {
                                setPhase(replicationTask, "finished");
                                super.handleResponse(response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                setPhase(replicationTask, "finished");
                                super.handleException(exp);
                            }
                        });
                } else {
                    setPhase(replicationTask, "primary");
                    final IndexMetaData indexMetaData = clusterService.state().getMetaData().index(request.shardId().getIndex());
                    final boolean executeOnReplicas = (indexMetaData == null) || shouldExecuteReplication(indexMetaData.getSettings());
                    final ActionListener<Response> listener = createResponseListener(primaryShardReference);
                    createReplicatedOperation(request, new ActionListener<PrimaryResult>() {
                        @Override
                        public void onResponse(PrimaryResult result) {
                            result.respond(listener);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    }, primaryShardReference, executeOnReplicas).execute();
                }
            } catch (Exception e) {
                Releasables.closeWhileHandlingException(primaryShardReference); // release shard operation lock before responding to caller
                onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            setPhase(replicationTask, "finished");
            try {
                channel.sendResponse(e);
            } catch (IOException inner) {
                inner.addSuppressed(e);
                logger.warn("failed to send response", inner);
            }
        }

        private ActionListener<Response> createResponseListener(final PrimaryShardReference primaryShardReference) {
            return new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    primaryShardReference.close(); // release shard operation lock before responding to caller
                    setPhase(replicationTask, "finished");
                    try {
                        channel.sendResponse(response);
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    primaryShardReference.close(); // release shard operation lock before responding to caller
                    setPhase(replicationTask, "finished");
                    try {
                        channel.sendResponse(e);
                    } catch (IOException e1) {
                        logger.warn("failed to send response", e);
                    }
                }
            };
        }

        protected ReplicationOperation<Request, ReplicaRequest, PrimaryResult> createReplicatedOperation(
            Request request, ActionListener<PrimaryResult> listener,
            PrimaryShardReference primaryShardReference, boolean executeOnReplicas) {
            return new ReplicationOperation<>(request, primaryShardReference, listener,
                executeOnReplicas, replicasProxy, clusterService::state, logger, actionName
            );
        }
    }

    protected class PrimaryResult implements ReplicationOperation.PrimaryResult<ReplicaRequest> {
        final ReplicaRequest replicaRequest;
        final Response finalResponse;

        public PrimaryResult(ReplicaRequest replicaRequest, Response finalResponse) {
            this.replicaRequest = replicaRequest;
            this.finalResponse = finalResponse;
        }

        @Override
        public ReplicaRequest replicaRequest() {
            return replicaRequest;
        }

        @Override
        public void setShardInfo(ReplicationResponse.ShardInfo shardInfo) {
            finalResponse.setShardInfo(shardInfo);
        }

        public void respond(ActionListener<Response> listener) {
            listener.onResponse(finalResponse);
        }
    }

    protected class ReplicaResult {
        /**
         * Public constructor so subclasses can call it.
         */
        public ReplicaResult() {}

        public void respond(ActionListener<TransportResponse.Empty> listener) {
            listener.onResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class ReplicaOperationTransportHandler implements TransportRequestHandler<ReplicaRequest> {
        @Override
        public void messageReceived(final ReplicaRequest request, final TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }

        @Override
        public void messageReceived(ReplicaRequest request, TransportChannel channel, Task task) throws Exception {
            new AsyncReplicaAction(request, channel, (ReplicationTask) task).run();
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

    private final class AsyncReplicaAction extends AbstractRunnable implements ActionListener<Releasable> {
        private final ReplicaRequest request;
        private final TransportChannel channel;
        /**
         * The task on the node with the replica shard.
         */
        private final ReplicationTask task;
        // important: we pass null as a timeout as failing a replica is
        // something we want to avoid at all costs
        private final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());

        AsyncReplicaAction(ReplicaRequest request, TransportChannel channel, ReplicationTask task) {
            this.request = request;
            this.channel = channel;
            this.task = task;
        }

        @Override
        public void onResponse(Releasable releasable) {
            try {
                ReplicaResult replicaResult = shardOperationOnReplica(request);
                releasable.close(); // release shard operation lock before responding to caller
                replicaResult.respond(new ResponseListener());
            } catch (Exception e) {
                Releasables.closeWhileHandlingException(releasable); // release shard operation lock before responding to caller
                AsyncReplicaAction.this.onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof RetryOnReplicaException) {
                logger.trace("Retrying operation on replica, action [{}], request [{}]", e, transportReplicaAction, request);
                final ThreadContext.StoredContext context = threadPool.getThreadContext().newStoredContext();
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        context.close();
                        // Forking a thread on local node via transport service so that custom transport service have an
                        // opportunity to execute custom logic before the replica operation begins
                        String extraMessage = "action [" + transportReplicaAction + "], request[" + request + "]";
                        TransportChannelResponseHandler<TransportResponse.Empty> handler =
                            new TransportChannelResponseHandler<>(logger, channel, extraMessage, () -> TransportResponse.Empty.INSTANCE);
                        transportService.sendRequest(clusterService.localNode(), transportReplicaAction, request, handler);
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
            try {
                setPhase(task, "finished");
                channel.sendResponse(e);
            } catch (IOException responseException) {
                responseException.addSuppressed(e);
                logger.warn("failed to send error message back to client for action [{}]", responseException, transportReplicaAction);
            }
        }

        @Override
        protected void doRun() throws Exception {
            setPhase(task, "replica");
            assert request.shardId() != null : "request shardId must be set";
            acquireReplicaOperationLock(request.shardId(), request.primaryTerm(), this);
        }

        /**
         * Listens for the response on the replica and sends the response back to the primary.
         */
        private class ResponseListener implements ActionListener<TransportResponse.Empty> {
            @Override
            public void onResponse(Empty response) {
                if (logger.isTraceEnabled()) {
                    logger.trace("action [{}] completed on shard [{}] for request [{}]", transportReplicaAction, request.shardId(),
                            request);
                }
                setPhase(task, "finished");
                try {
                    channel.sendResponse(response);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                responseWithFailure(e);
            }
        }
    }

    /**
     * Responsible for routing and retrying failed operations on the primary.
     * The actual primary operation is done in {@link ReplicationOperation} on the
     * node with primary copy.
     *
     * Resolves index and shard id for the request before routing it to target node
     */
    final class ReroutePhase extends AbstractRunnable {
        private final ActionListener<Response> listener;
        private final Request request;
        private final ReplicationTask task;
        private final ClusterStateObserver observer;
        private final AtomicBoolean finished = new AtomicBoolean();

        ReroutePhase(ReplicationTask task, Request request, ActionListener<Response> listener) {
            this.request = request;
            if (task != null) {
                this.request.setParentTask(clusterService.localNode().getId(), task.getId());
            }
            this.listener = listener;
            this.task = task;
            this.observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
        }

        @Override
        public void onFailure(Exception e) {
            finishWithUnexpectedFailure(e);
        }

        @Override
        protected void doRun() {
            setPhase(task, "routing");
            final ClusterState state = observer.observedState();
            if (handleBlockExceptions(state)) {
                return;
            }

            // request does not have a shardId yet, we need to pass the concrete index to resolve shardId
            final String concreteIndex = concreteIndex(state);
            final IndexMetaData indexMetaData = state.metaData().index(concreteIndex);
            if (indexMetaData == null) {
                retry(new IndexNotFoundException(concreteIndex));
                return;
            }

            // resolve all derived request fields, so we can route and apply it
            resolveRequest(state.metaData(), indexMetaData, request);
            assert request.shardId() != null : "request shardId must be set in resolveRequest";
            assert request.waitForActiveShards() != ActiveShardCount.DEFAULT : "request waitForActiveShards must be set in resolveRequest";

            final ShardRouting primary = primary(state);
            if (retryIfUnavailable(state, primary)) {
                return;
            }
            final DiscoveryNode node = state.nodes().get(primary.currentNodeId());
            taskManager.registerChildTask(task, node.getId());
            if (primary.currentNodeId().equals(state.nodes().getLocalNodeId())) {
                performLocalAction(state, primary, node);
            } else {
                performRemoteAction(state, primary, node);
            }
        }

        private void performLocalAction(ClusterState state, ShardRouting primary, DiscoveryNode node) {
            setPhase(task, "waiting_on_primary");
            if (logger.isTraceEnabled()) {
                logger.trace("send action [{}] on primary [{}] for request [{}] with cluster state version [{}] to [{}] ",
                    transportPrimaryAction, request.shardId(), request, state.version(), primary.currentNodeId());
            }
            performAction(node, transportPrimaryAction, true);
        }

        private void performRemoteAction(ClusterState state, ShardRouting primary, DiscoveryNode node) {
            if (state.version() < request.routedBasedOnClusterVersion()) {
                logger.trace("failed to find primary [{}] for request [{}] despite sender thinking it would be here. Local cluster state "
                        + "version [{}]] is older than on sending node (version [{}]), scheduling a retry...", request.shardId(), request,
                    state.version(), request.routedBasedOnClusterVersion());
                retryBecauseUnavailable(request.shardId(), "failed to find primary as current cluster state with version ["
                    + state.version() + "] is stale (expected at least [" + request.routedBasedOnClusterVersion() + "]");
                return;
            } else {
                // chasing the node with the active primary for a second hop requires that we are at least up-to-date with the current
                // cluster state version this prevents redirect loops between two nodes when a primary was relocated and the relocation
                // target is not aware that it is the active primary shard already.
                request.routedBasedOnClusterVersion(state.version());
            }
            if (logger.isTraceEnabled()) {
                logger.trace("send action [{}] on primary [{}] for request [{}] with cluster state version [{}] to [{}]", actionName,
                    request.shardId(), request, state.version(), primary.currentNodeId());
            }
            setPhase(task, "rerouted");
            performAction(node, actionName, false);
        }

        private boolean retryIfUnavailable(ClusterState state, ShardRouting primary) {
            if (primary == null || primary.active() == false) {
                logger.trace("primary shard [{}] is not yet active, scheduling a retry: action [{}], request [{}], "
                    + "cluster state version [{}]", request.shardId(), actionName, request, state.version());
                retryBecauseUnavailable(request.shardId(), "primary shard is not active");
                return true;
            }
            if (state.nodes().nodeExists(primary.currentNodeId()) == false) {
                logger.trace("primary shard [{}] is assigned to an unknown node [{}], scheduling a retry: action [{}], request [{}], "
                    + "cluster state version [{}]", request.shardId(), primary.currentNodeId(), actionName, request, state.version());
                retryBecauseUnavailable(request.shardId(), "primary shard isn't assigned to a known node.");
                return true;
            }
            return false;
        }

        private String concreteIndex(ClusterState state) {
            return resolveIndex() ? indexNameExpressionResolver.concreteSingleIndex(state, request).getName() : request.index();
        }

        private ShardRouting primary(ClusterState state) {
            IndexShardRoutingTable indexShard = state.getRoutingTable().shardRoutingTable(request.shardId());
            return indexShard.primaryShard();
        }

        private boolean handleBlockExceptions(ClusterState state) {
            ClusterBlockException blockException = state.blocks().globalBlockedException(globalBlockLevel());
            if (blockException != null) {
                handleBlockException(blockException);
                return true;
            }
            blockException = state.blocks().indexBlockedException(indexBlockLevel(), concreteIndex(state));
            if (blockException != null) {
                handleBlockException(blockException);
                return true;
            }
            return false;
        }

        private void handleBlockException(ClusterBlockException blockException) {
            if (blockException.retryable()) {
                logger.trace("cluster is blocked, scheduling a retry", blockException);
                retry(blockException);
            } else {
                finishAsFailed(blockException);
            }
        }

        private void performAction(final DiscoveryNode node, final String action, final boolean isPrimaryAction) {
            transportService.sendRequest(node, action, request, transportOptions, new TransportResponseHandler<Response>() {

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
                    finishOnSuccess(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    try {
                        // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                        final Throwable cause = exp.unwrapCause();
                        if (cause instanceof ConnectTransportException || cause instanceof NodeClosedException ||
                            (isPrimaryAction && retryPrimaryException(cause))) {
                            logger.trace("received an error from node [{}] for request [{}], scheduling a retry", exp, node.getId(),
                                request);
                            retry(exp);
                        } else {
                            finishAsFailed(exp);
                        }
                    } catch (Exception e) {
                        e.addSuppressed(exp);
                        finishWithUnexpectedFailure(e);
                    }
                }
            });
        }

        void retry(Exception failure) {
            assert failure != null;
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                finishAsFailed(failure);
                return;
            }
            setPhase(task, "waiting_for_retry");
            final ThreadContext.StoredContext context = threadPool.getThreadContext().newStoredContext();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    context.close();
                    run();
                }

                @Override
                public void onClusterServiceClose() {
                    finishAsFailed(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    context.close();
                    // Try one more time...
                    run();
                }
            });
        }

        void finishAsFailed(Exception failure) {
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "failed");
                logger.trace("operation failed. action [{}], request [{}]", failure, actionName, request);
                listener.onFailure(failure);
            } else {
                assert false : "finishAsFailed called but operation is already finished";
            }
        }

        void finishWithUnexpectedFailure(Exception failure) {
            logger.warn("unexpected error during the primary phase for action [{}], request [{}]", failure, actionName, request);
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "failed");
                listener.onFailure(failure);
            } else {
                assert false : "finishWithUnexpectedFailure called but operation is already finished";
            }
        }

        void finishOnSuccess(Response response) {
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "finished");
                if (logger.isTraceEnabled()) {
                    logger.trace("operation succeeded. action [{}],request [{}]", actionName, request);
                }
                listener.onResponse(response);
            } else {
                assert false : "finishOnSuccess called but operation is already finished";
            }
        }

        void retryBecauseUnavailable(ShardId shardId, String message) {
            retry(new UnavailableShardsException(shardId, "{} Timeout: [{}], request: [{}]", message, request.timeout(), request));
        }
    }

    /**
     * tries to acquire reference to {@link IndexShard} to perform a primary operation. Released after performing primary operation locally
     * and replication of the operation to all replica shards is completed / failed (see {@link ReplicationOperation}).
     */
    protected void acquirePrimaryShardReference(ShardId shardId, ActionListener<PrimaryShardReference> onReferenceAcquired) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        // we may end up here if the cluster state used to route the primary is so stale that the underlying
        // index shard was replaced with a replica. For example - in a two node cluster, if the primary fails
        // the replica will take over and a replica will be assigned to the first node.
        if (indexShard.routingEntry().primary() == false) {
            throw new ReplicationOperation.RetryOnPrimaryException(indexShard.shardId(),
                "actual shard is not a primary " + indexShard.routingEntry());
        }

        ActionListener<Releasable> onAcquired = new ActionListener<Releasable>() {
            @Override
            public void onResponse(Releasable releasable) {
                onReferenceAcquired.onResponse(new PrimaryShardReference(indexShard, releasable));
            }

            @Override
            public void onFailure(Exception e) {
                onReferenceAcquired.onFailure(e);
            }
        };

        indexShard.acquirePrimaryOperationLock(onAcquired, executor);
    }

    /**
     * tries to acquire an operation on replicas. The lock is closed as soon as replication is completed on the node.
     */
    protected void acquireReplicaOperationLock(ShardId shardId, long primaryTerm, ActionListener<Releasable> onLockAcquired) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        indexShard.acquireReplicaOperationLock(primaryTerm, onLockAcquired, executor);
    }

    /**
     * Indicated whether this operation should be replicated to shadow replicas or not. If this method returns true the replication phase
     * will be skipped. For example writes such as index and delete don't need to be replicated on shadow replicas but refresh and flush do.
     */
    protected boolean shouldExecuteReplication(Settings settings) {
        return IndexMetaData.isIndexUsingShadowReplicas(settings) == false;
    }

    class PrimaryShardReference implements ReplicationOperation.Primary<Request, ReplicaRequest, PrimaryResult>, Releasable {

        private final IndexShard indexShard;
        private final Releasable operationLock;

        PrimaryShardReference(IndexShard indexShard, Releasable operationLock) {
            this.indexShard = indexShard;
            this.operationLock = operationLock;
        }

        @Override
        public void close() {
            operationLock.close();
        }

        public boolean isRelocated() {
            return indexShard.state() == IndexShardState.RELOCATED;
        }

        @Override
        public void failShard(String reason, Exception e) {
            try {
                indexShard.failShard(reason, e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
        }

        @Override
        public PrimaryResult perform(Request request) throws Exception {
            PrimaryResult result = shardOperationOnPrimary(request);
            result.replicaRequest().primaryTerm(indexShard.getPrimaryTerm());
            return result;
        }

        @Override
        public ShardRouting routingEntry() {
            return indexShard.routingEntry();
        }
    }

    final class ReplicasProxy implements ReplicationOperation.Replicas<ReplicaRequest> {

        @Override
        public void performOn(ShardRouting replica, ReplicaRequest request, ActionListener<TransportResponse.Empty> listener) {
            String nodeId = replica.currentNodeId();
            final DiscoveryNode node = clusterService.state().nodes().get(nodeId);
            if (node == null) {
                listener.onFailure(new NoNodeAvailableException("unknown node [" + nodeId + "]"));
                return;
            }
            transportService.sendRequest(node, transportReplicaAction, request, transportOptions,
                new ActionListenerResponseHandler<>(listener, () -> TransportResponse.Empty.INSTANCE));
        }

        @Override
        public void failShard(ShardRouting replica, long primaryTerm, String message, Exception exception,
                              Runnable onSuccess, Consumer<Exception> onFailure, Consumer<Exception> onIgnoredFailure) {
            shardStateAction.remoteShardFailed(
                replica, primaryTerm, message, exception,
                new ShardStateAction.Listener() {
                    @Override
                    public void onSuccess() {
                        onSuccess.run();
                    }

                    @Override
                    public void onFailure(Exception shardFailedError) {
                        if (shardFailedError instanceof ShardStateAction.NoLongerPrimaryShardException) {
                            onFailure.accept(shardFailedError);
                        } else {
                            // these can occur if the node is shutting down and are okay
                            // any other exception here is not expected and merits investigation
                            assert shardFailedError instanceof TransportException ||
                                shardFailedError instanceof NodeClosedException : shardFailedError;
                            onIgnoredFailure.accept(shardFailedError);
                        }
                    }
                }
            );
        }
    }

    /**
     * Sets the current phase on the task if it isn't null. Pulled into its own
     * method because its more convenient that way.
     */
    static void setPhase(ReplicationTask task, String phase) {
        if (task != null) {
            task.setPhase(phase);
        }
    }
}
