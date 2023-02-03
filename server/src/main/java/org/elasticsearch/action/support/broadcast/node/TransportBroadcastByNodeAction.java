/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast.node;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.core.Strings.format;

/**
 * Abstraction for transporting aggregated shard-level operations in a single request (NodeRequest) per-node
 * and executing the shard-level operations serially on the receiving node. Each shard-level operation can produce a
 * result (ShardOperationResult), these per-node shard-level results are aggregated into a single result
 * (BroadcastByNodeResponse) to the coordinating node. These per-node results are aggregated into a single result (Result)
 * to the client.
 *
 * @param <Request>              the underlying client request
 * @param <Response>             the response to the client request
 * @param <ShardOperationResult> per-shard operation results
 */
public abstract class TransportBroadcastByNodeAction<
    Request extends BroadcastRequest<Request>,
    Response extends BaseBroadcastResponse,
    ShardOperationResult extends Writeable> extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportBroadcastByNodeAction.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    final String transportNodeBroadcastAction;

    public TransportBroadcastByNodeAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        String executor
    ) {
        this(actionName, clusterService, transportService, actionFilters, indexNameExpressionResolver, request, executor, true);
    }

    public TransportBroadcastByNodeAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        String executor,
        boolean canTripCircuitBreaker
    ) {
        super(actionName, canTripCircuitBreaker, transportService, actionFilters, request);

        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        transportNodeBroadcastAction = actionName + "[n]";

        transportService.registerRequestHandler(
            transportNodeBroadcastAction,
            executor,
            false,
            canTripCircuitBreaker,
            NodeRequest::new,
            new BroadcastByNodeTransportRequestHandler()
        );
    }

    /**
     * Deserialize a shard-level result from an input stream
     *
     * @param in input stream
     * @return a deserialized shard-level result
     */
    protected abstract ShardOperationResult readShardResult(StreamInput in) throws IOException;

    public interface ResponseFactory<Response, ShardOperationResult> {
        /**
         * Creates a new response to the underlying request.
         *
         * @param totalShards      the total number of shards considered for execution of the operation
         * @param successfulShards the total number of shards for which execution of the operation was successful
         * @param failedShards     the total number of shards for which execution of the operation failed
         * @param results          the per-node aggregated shard-level results
         * @param shardFailures    the exceptions corresponding to shard operation failures
         * @return the response
         */
        Response newResponse(
            int totalShards,
            int successfulShards,
            int failedShards,
            List<ShardOperationResult> results,
            List<DefaultShardOperationFailedException> shardFailures
        );
    }

    /**
     * Create a response factory based on the requst and the cluster state captured at the time the request was handled. Implementations
     * must avoid capturing the full cluster state if possible.
     */
    protected abstract ResponseFactory<Response, ShardOperationResult> getResponseFactory(Request request, ClusterState clusterState);

    /**
     * Deserialize a request from an input stream
     *
     * @param in input stream
     * @return a de-serialized request
     */
    protected abstract Request readRequestFrom(StreamInput in) throws IOException;

    /**
     * Executes the shard-level operation. This method is called once per shard serially on the receiving node.
     * This method should not throw an exception, but pass the exception to the listener instead.
     *
     * @param request      the node-level request
     * @param shardRouting the shard on which to execute the operation
     * @param task         the task for this node-level request
     * @param listener     the listener to notify with the result of the shard-level operation
     */
    protected abstract void shardOperation(
        Request request,
        ShardRouting shardRouting,
        Task task,
        ActionListener<ShardOperationResult> listener
    );

    /**
     * Determines the shards on which this operation will be executed on. The operation is executed once per shard.
     *
     * @param clusterState    the cluster state
     * @param request         the underlying request
     * @param concreteIndices the concrete indices on which to execute the operation
     * @return the shards on which to execute the operation
     */
    protected abstract ShardsIterator shards(ClusterState clusterState, Request request, String[] concreteIndices);

    /**
     * Executes a global block check before polling the cluster state.
     *
     * @param state   the cluster state
     * @param request the underlying request
     * @return a non-null exception if the operation is blocked
     */
    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    /**
     * Executes a global request-level check before polling the cluster state.
     *
     * @param state           the cluster state
     * @param request         the underlying request
     * @param concreteIndices the concrete indices on which to execute the operation
     * @return a non-null exception if the operation if blocked
     */
    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices);

    /**
     * Resolves a list of concrete index names. Override this if index names should be resolved differently than normal.
     *
     * @param clusterState the cluster state
     * @param request the underlying request
     * @return a list of concrete index names that this action should operate on
     */
    protected String[] resolveConcreteIndexNames(ClusterState clusterState, Request request) {
        return indexNameExpressionResolver.concreteIndexNames(clusterState, request);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final var clusterState = clusterService.state();

        final var globalBlockException = checkGlobalBlock(clusterState, request);
        if (globalBlockException != null) {
            throw globalBlockException;
        }

        final var concreteIndices = resolveConcreteIndexNames(clusterState, request);
        final var requestBlockException = checkRequestBlock(clusterState, request, concreteIndices);
        if (requestBlockException != null) {
            throw requestBlockException;
        }

        logger.trace(() -> format("resolving shards for [%s] based on cluster state version [%s]", actionName, clusterState.version()));
        final ShardsIterator shardIt = shards(clusterState, request, concreteIndices);
        final Map<String, List<ShardRouting>> shardsByNodeId = new HashMap<>();

        final var nodes = clusterState.nodes();
        int unavailableShardCount = 0;
        int availableShardCount = 0;
        for (final var shard : shardIt) {
            // send a request to the shard only if it is assigned to a node that is in the local node's cluster state
            // a scenario in which a shard can be assigned but to a node that is not in the local node's cluster state
            // is when the shard is assigned to the master node, the local node has detected the master as failed
            // and a new master has not yet been elected; in this situation the local node will have removed the
            // master node from the local cluster state, but the shards assigned to the master will still be in the
            // routing table as such
            final var nodeId = shard.currentNodeId();
            if (nodeId != null && nodes.get(nodeId) != null) {
                shardsByNodeId.computeIfAbsent(nodeId, n -> new ArrayList<>()).add(shard);
                availableShardCount += 1;
            } else {
                unavailableShardCount++;
            }
        }

        executeAsCoordinatingNode(
            task,
            request,
            shardsByNodeId,
            unavailableShardCount,
            availableShardCount,
            nodes,
            getResponseFactory(request, clusterState),
            listener
        );
    }

    private void executeAsCoordinatingNode(
        Task task,
        Request request,
        Map<String, List<ShardRouting>> shardsByNodeId,
        int unavailableShardCount,
        int availableShardCount,
        DiscoveryNodes nodes,
        ResponseFactory<Response, ShardOperationResult> responseFactory,
        ActionListener<Response> listener
    ) {
        final var mutex = new Object();
        final var shardResponses = new ArrayList<ShardOperationResult>(availableShardCount);
        final var exceptions = new ArrayList<DefaultShardOperationFailedException>(0);
        final var totalShards = new AtomicInteger(unavailableShardCount);
        final var successfulShards = new AtomicInteger(0);

        final var resultListener = new ListenableFuture<Response>();
        final var resultListenerCompleter = new RunOnce(() -> {
            if (task instanceof CancellableTask cancellableTask) {
                if (cancellableTask.notifyIfCancelled(resultListener)) {
                    return;
                }
            }
            // ref releases all happen-before here so no need to be synchronized
            resultListener.onResponse(
                responseFactory.newResponse(totalShards.get(), successfulShards.get(), exceptions.size(), shardResponses, exceptions)
            );
        });

        final var nodeFailureListeners = new ListenableFuture<NodeResponse>();
        if (task instanceof CancellableTask cancellableTask) {
            cancellableTask.addListener(() -> {
                assert cancellableTask.isCancelled();
                resultListenerCompleter.run();
                cancellableTask.notifyIfCancelled(nodeFailureListeners);
            });
        }

        final var transportRequestOptions = TransportRequestOptions.timeout(request.timeout());

        try (var refs = new RefCountingRunnable(() -> {
            resultListener.addListener(listener);
            resultListenerCompleter.run();
        })) {
            for (final var entry : shardsByNodeId.entrySet()) {
                final var node = nodes.get(entry.getKey());
                final var shards = entry.getValue();

                final ActionListener<NodeResponse> nodeResponseListener = ActionListener.notifyOnce(new ActionListener<NodeResponse>() {
                    @Override
                    public void onResponse(NodeResponse nodeResponse) {
                        synchronized (mutex) {
                            shardResponses.addAll(nodeResponse.getResults());
                        }
                        totalShards.addAndGet(nodeResponse.getTotalShards());
                        successfulShards.addAndGet(nodeResponse.getSuccessfulShards());

                        for (BroadcastShardOperationFailedException exception : nodeResponse.getExceptions()) {
                            if (TransportActions.isShardNotAvailableException(exception)) {
                                assert node.getVersion().before(Version.V_8_7_0) : node; // we stopped sending these ignored exceptions
                            } else {
                                synchronized (mutex) {
                                    exceptions.add(
                                        new DefaultShardOperationFailedException(
                                            exception.getShardId().getIndexName(),
                                            exception.getShardId().getId(),
                                            exception
                                        )
                                    );
                                }
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled()) {
                            return;
                        }

                        logger.debug(() -> format("failed to execute [%s] on node [%s]", actionName, node), e);

                        final var failedNodeException = new FailedNodeException(node.getId(), "Failed node [" + node.getId() + "]", e);
                        synchronized (mutex) {
                            for (ShardRouting shard : shards) {
                                exceptions.add(
                                    new DefaultShardOperationFailedException(shard.getIndexName(), shard.getId(), failedNodeException)
                                );
                            }
                        }

                        totalShards.addAndGet(shards.size());
                    }

                    @Override
                    public String toString() {
                        return "[" + actionName + "][" + node.descriptionWithoutAttributes() + "]";
                    }
                });

                if (task instanceof CancellableTask) {
                    nodeFailureListeners.addListener(nodeResponseListener);
                }

                final var nodeRequest = new NodeRequest(request, shards, node.getId());
                if (task != null) {
                    nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                }

                transportService.sendRequest(
                    node,
                    transportNodeBroadcastAction,
                    nodeRequest,
                    transportRequestOptions,
                    new ActionListenerResponseHandler<>(
                        ActionListener.releaseAfter(nodeResponseListener, refs.acquire()),
                        NodeResponse::new
                    )
                );
            }
        }
    }

    class BroadcastByNodeTransportRequestHandler implements TransportRequestHandler<NodeRequest> {
        @Override
        public void messageReceived(final NodeRequest request, TransportChannel channel, Task task) throws Exception {
            executeAsDataNode(
                task,
                request.getIndicesLevelRequest(),
                request.getShards(),
                request.getNodeId(),
                new ChannelActionListener<>(channel, transportNodeBroadcastAction, request)
            );
        }
    }

    private void executeAsDataNode(
        Task task,
        Request request,
        List<ShardRouting> shards,
        String nodeId,
        ActionListener<NodeResponse> listener
    ) {
        logger.trace("[{}] executing operation on [{}] shards", actionName, shards.size());

        final var results = new ArrayList<ShardOperationResult>(shards.size());
        final var exceptions = new ArrayList<BroadcastShardOperationFailedException>(0);

        final var resultListener = new ListenableFuture<NodeResponse>();
        final var resultListenerCompleter = new RunOnce(() -> {
            if (task instanceof CancellableTask cancellableTask) {
                if (cancellableTask.notifyIfCancelled(resultListener)) {
                    return;
                }
            }
            // ref releases all happen-before here so no need to be synchronized
            resultListener.onResponse(new NodeResponse(nodeId, shards.size(), results, exceptions));
        });

        final var shardFailureListeners = new ListenableFuture<ShardOperationResult>();
        if (task instanceof CancellableTask cancellableTask) {
            cancellableTask.addListener(() -> {
                assert cancellableTask.isCancelled();
                resultListenerCompleter.run();
                cancellableTask.notifyIfCancelled(shardFailureListeners);
            });
        }

        try (var refs = new RefCountingRunnable(() -> {
            resultListener.addListener(listener);
            resultListenerCompleter.run();
        })) {
            for (final var shardRouting : shards) {
                if (task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled()) {
                    return;
                }

                final ActionListener<ShardOperationResult> shardListener = ActionListener.notifyOnce(new ActionListener<>() {
                    @Override
                    public void onResponse(ShardOperationResult shardOperationResult) {
                        logger.trace(() -> format("[%s] completed operation for shard [%s]", actionName, shardRouting.shortSummary()));
                        synchronized (results) {
                            results.add(shardOperationResult);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled()) {
                            return;
                        }
                        logger.log(
                            TransportActions.isShardNotAvailableException(e) ? Level.TRACE : Level.DEBUG,
                            () -> format("[%s] failed to execute operation for shard [%s]", actionName, shardRouting.shortSummary()),
                            e
                        );
                        if (TransportActions.isShardNotAvailableException(e) == false) {
                            synchronized (exceptions) {
                                exceptions.add(
                                    new BroadcastShardOperationFailedException(
                                        shardRouting.shardId(),
                                        "operation " + actionName + " failed",
                                        e
                                    )
                                );
                            }
                        }
                    }

                    @Override
                    public String toString() {
                        return "[" + actionName + "][" + shardRouting + "]";
                    }
                });

                if (task instanceof CancellableTask) {
                    shardFailureListeners.addListener(shardListener);
                }

                logger.trace(() -> format("[%s] executing operation for shard [%s]", actionName, shardRouting.shortSummary()));
                ActionRunnable.wrap(
                    ActionListener.releaseAfter(shardListener, refs.acquire()),
                    l -> shardOperation(request, shardRouting, task, l)
                ).run();
            }
        }
    }

    class NodeRequest extends TransportRequest implements IndicesRequest {
        private final Request indicesLevelRequest;
        private final List<ShardRouting> shards;
        private final String nodeId;

        NodeRequest(StreamInput in) throws IOException {
            super(in);
            indicesLevelRequest = readRequestFrom(in);
            shards = in.readList(ShardRouting::new);
            nodeId = in.readString();
        }

        NodeRequest(Request indicesLevelRequest, List<ShardRouting> shards, String nodeId) {
            this.indicesLevelRequest = indicesLevelRequest;
            this.shards = shards;
            this.nodeId = nodeId;
        }

        List<ShardRouting> getShards() {
            return shards;
        }

        String getNodeId() {
            return nodeId;
        }

        Request getIndicesLevelRequest() {
            return indicesLevelRequest;
        }

        @Override
        public String[] indices() {
            return indicesLevelRequest.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesLevelRequest.indicesOptions();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            indicesLevelRequest.writeTo(out);
            out.writeList(shards);
            out.writeString(nodeId);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return indicesLevelRequest.createTask(id, type, action, parentTaskId, headers);
        }
    }

    class NodeResponse extends TransportResponse {
        protected String nodeId;
        protected int totalShards;
        protected List<BroadcastShardOperationFailedException> exceptions;
        protected List<ShardOperationResult> results;

        NodeResponse(StreamInput in) throws IOException {
            super(in);
            nodeId = in.readString();
            totalShards = in.readVInt();
            results = in.readList((stream) -> stream.readBoolean() ? readShardResult(stream) : null);
            if (in.readBoolean()) {
                exceptions = in.readList(BroadcastShardOperationFailedException::new);
            } else {
                exceptions = null;
            }
        }

        NodeResponse(
            String nodeId,
            int totalShards,
            List<ShardOperationResult> results,
            List<BroadcastShardOperationFailedException> exceptions
        ) {
            this.nodeId = nodeId;
            this.totalShards = totalShards;
            this.results = results;
            this.exceptions = exceptions;
        }

        String getNodeId() {
            return nodeId;
        }

        int getTotalShards() {
            return totalShards;
        }

        int getSuccessfulShards() {
            return results.size();
        }

        List<ShardOperationResult> getResults() {
            return results;
        }

        List<BroadcastShardOperationFailedException> getExceptions() {
            return exceptions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeVInt(totalShards);
            out.writeCollection(results, StreamOutput::writeOptionalWriteable);
            out.writeBoolean(exceptions != null);
            if (exceptions != null) {
                out.writeList(exceptions);
            }
        }
    }

    /**
     * Can be used for implementations of {@link #shardOperation(BroadcastRequest, ShardRouting, Task, ActionListener) shardOperation} for
     * which there is no shard-level return value.
     */
    public static final class EmptyResult implements Writeable {
        public static EmptyResult INSTANCE = new EmptyResult();

        private EmptyResult() {}

        @Override
        public void writeTo(StreamOutput out) {}
    }
}
