/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast.node;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

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
public abstract class TransportBroadcastByNodeAction<Request extends BroadcastRequest<Request>,
        Response extends BroadcastResponse,
        ShardOperationResult extends Writeable> extends HandledTransportAction<Request, Response> {

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
        String executor) {
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
            boolean canTripCircuitBreaker) {
        super(actionName, canTripCircuitBreaker, transportService, actionFilters, request);

        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        transportNodeBroadcastAction = actionName + "[n]";

        transportService.registerRequestHandler(transportNodeBroadcastAction, executor, false, canTripCircuitBreaker, NodeRequest::new,
            new BroadcastByNodeTransportRequestHandler());
    }

    private Response newResponse(
            Request request,
            AtomicReferenceArray<?> responses,
            List<NoShardAvailableActionException> unavailableShardExceptions,
            Map<String, List<ShardRouting>> nodes,
            ClusterState clusterState) {
        int totalShards = 0;
        int successfulShards = 0;
        List<ShardOperationResult> broadcastByNodeResponses = new ArrayList<>();
        List<DefaultShardOperationFailedException> exceptions = new ArrayList<>();
        for (int i = 0; i < responses.length(); i++) {
            if (responses.get(i) instanceof FailedNodeException) {
                FailedNodeException exception = (FailedNodeException) responses.get(i);
                totalShards += nodes.get(exception.nodeId()).size();
                for (ShardRouting shard : nodes.get(exception.nodeId())) {
                    exceptions.add(new DefaultShardOperationFailedException(shard.getIndexName(), shard.getId(), exception));
                }
            } else {
                @SuppressWarnings("unchecked")
                NodeResponse response = (NodeResponse) responses.get(i);
                broadcastByNodeResponses.addAll(response.results);
                totalShards += response.getTotalShards();
                successfulShards += response.getSuccessfulShards();
                for (BroadcastShardOperationFailedException throwable : response.getExceptions()) {
                    if (TransportActions.isShardNotAvailableException(throwable) == false) {
                        exceptions.add(new DefaultShardOperationFailedException(throwable.getShardId().getIndexName(),
                            throwable.getShardId().getId(), throwable));
                    }
                }
            }
        }
        totalShards += unavailableShardExceptions.size();
        int failedShards = exceptions.size();
        return newResponse(request, totalShards, successfulShards, failedShards, broadcastByNodeResponses, exceptions, clusterState);
    }

    /**
     * Deserialize a shard-level result from an input stream
     *
     * @param in input stream
     * @return a deserialized shard-level result
     */
    protected abstract ShardOperationResult readShardResult(StreamInput in) throws IOException;

    /**
     * Creates a new response to the underlying request.
     *
     * @param request          the underlying request
     * @param totalShards      the total number of shards considered for execution of the operation
     * @param successfulShards the total number of shards for which execution of the operation was successful
     * @param failedShards     the total number of shards for which execution of the operation failed
     * @param results          the per-node aggregated shard-level results
     * @param shardFailures    the exceptions corresponding to shard operation failures
     * @param clusterState     the cluster state
     * @return the response
     */
    protected abstract Response newResponse(Request request, int totalShards, int successfulShards, int failedShards,
                                            List<ShardOperationResult> results, List<DefaultShardOperationFailedException> shardFailures,
                                            ClusterState clusterState);

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
    protected abstract void shardOperation(Request request, ShardRouting shardRouting, Task task,
                                           ActionListener<ShardOperationResult> listener);

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
        new AsyncAction(task, request, listener).start();
    }

    protected class AsyncAction {
        private final Task task;
        private final Request request;
        private final ActionListener<Response> listener;
        private final ClusterState clusterState;
        private final DiscoveryNodes nodes;
        private final Map<String, List<ShardRouting>> nodeIds;
        private final AtomicReferenceArray<Object> responses;
        private final AtomicInteger counter = new AtomicInteger();
        private List<NoShardAvailableActionException> unavailableShardExceptions = new ArrayList<>();

        protected AsyncAction(Task task, Request request, ActionListener<Response> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;

            clusterState = clusterService.state();
            nodes = clusterState.nodes();

            ClusterBlockException globalBlockException = checkGlobalBlock(clusterState, request);
            if (globalBlockException != null) {
                throw globalBlockException;
            }

            String[] concreteIndices = resolveConcreteIndexNames(clusterState, request);
            ClusterBlockException requestBlockException = checkRequestBlock(clusterState, request, concreteIndices);
            if (requestBlockException != null) {
                throw requestBlockException;
            }

            if (logger.isTraceEnabled()) {
                logger.trace("resolving shards for [{}] based on cluster state version [{}]", actionName, clusterState.version());
            }
            ShardsIterator shardIt = shards(clusterState, request, concreteIndices);
            nodeIds = new HashMap<>();

            for (ShardRouting shard : shardIt) {
                // send a request to the shard only if it is assigned to a node that is in the local node's cluster state
                // a scenario in which a shard can be assigned but to a node that is not in the local node's cluster state
                // is when the shard is assigned to the master node, the local node has detected the master as failed
                // and a new master has not yet been elected; in this situation the local node will have removed the
                // master node from the local cluster state, but the shards assigned to the master will still be in the
                // routing table as such
                if (shard.assignedToNode() && nodes.get(shard.currentNodeId()) != null) {
                    String nodeId = shard.currentNodeId();
                    if (nodeIds.containsKey(nodeId) == false) {
                        nodeIds.put(nodeId, new ArrayList<>());
                    }
                    nodeIds.get(nodeId).add(shard);
                } else {
                    unavailableShardExceptions.add(
                            new NoShardAvailableActionException(
                                    shard.shardId(),
                                    " no shards available for shard " + shard.toString() + " while executing " + actionName
                            )
                    );
                }
            }

            responses = new AtomicReferenceArray<>(nodeIds.size());
        }

        public void start() {
            if (nodeIds.size() == 0) {
                try {
                    onCompletion();
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            } else {
                int nodeIndex = -1;
                for (Map.Entry<String, List<ShardRouting>> entry : nodeIds.entrySet()) {
                    nodeIndex++;
                    DiscoveryNode node = nodes.get(entry.getKey());
                    sendNodeRequest(node, entry.getValue(), nodeIndex);
                }
            }
        }

        private void sendNodeRequest(final DiscoveryNode node, List<ShardRouting> shards, final int nodeIndex) {
            try {
                final NodeRequest nodeRequest = new NodeRequest(node.getId(), request, shards);
                if (task != null) {
                    nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                }

                final TransportRequestOptions transportRequestOptions = TransportRequestOptions.timeout(request.timeout());

                transportService.sendRequest(node, transportNodeBroadcastAction, nodeRequest, transportRequestOptions,
                        new TransportResponseHandler<NodeResponse>() {
                            @Override
                            public NodeResponse read(StreamInput in) throws IOException {
                                return new NodeResponse(in);
                            }

                            @Override
                            public void handleResponse(NodeResponse response) {
                                onNodeResponse(node, nodeIndex, response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                onNodeFailure(node, nodeIndex, exp);
                            }
                        });
            } catch (Exception e) {
                onNodeFailure(node, nodeIndex, e);
            }
        }

        protected void onNodeResponse(DiscoveryNode node, int nodeIndex, NodeResponse response) {
            if (logger.isTraceEnabled()) {
                logger.trace("received response for [{}] from node [{}]", actionName, node.getId());
            }

            // this is defensive to protect against the possibility of double invocation
            // the current implementation of TransportService#sendRequest guards against this
            // but concurrency is hard, safety is important, and the small performance loss here does not matter
            if (responses.compareAndSet(nodeIndex, null, response)) {
                if (counter.incrementAndGet() == responses.length()) {
                    onCompletion();
                }
            }
        }

        protected void onNodeFailure(DiscoveryNode node, int nodeIndex, Throwable t) {
            String nodeId = node.getId();
            logger.debug(new ParameterizedMessage("failed to execute [{}] on node [{}]", actionName, nodeId), t);

            // this is defensive to protect against the possibility of double invocation
            // the current implementation of TransportService#sendRequest guards against this
            // but concurrency is hard, safety is important, and the small performance loss here does not matter
            if (responses.compareAndSet(nodeIndex, null, new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", t))) {
                if (counter.incrementAndGet() == responses.length()) {
                    onCompletion();
                }
            }
        }

        protected void onCompletion() {
            if (task instanceof CancellableTask && ((CancellableTask)task).notifyIfCancelled(listener)) {
                return;
            }

            Response response = null;
            try {
                response = newResponse(request, responses, unavailableShardExceptions, nodeIds, clusterState);
            } catch (Exception e) {
                logger.debug("failed to combine responses from nodes", e);
                listener.onFailure(e);
            }
            if (response != null) {
                try {
                    listener.onResponse(response);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }
    }

    class BroadcastByNodeTransportRequestHandler implements TransportRequestHandler<NodeRequest> {
        @Override
        public void messageReceived(final NodeRequest request, TransportChannel channel, Task task) throws Exception {
            List<ShardRouting> shards = request.getShards();
            final int totalShards = shards.size();
            if (logger.isTraceEnabled()) {
                logger.trace("[{}] executing operation on [{}] shards", actionName, totalShards);
            }
            final AtomicArray<Object> shardResultOrExceptions = new AtomicArray<>(totalShards);

            final AtomicInteger counter = new AtomicInteger(shards.size());
            int shardIndex = -1;
            for (final ShardRouting shardRouting : shards) {
                shardIndex++;
                final int finalShardIndex = shardIndex;
                onShardOperation(request, shardRouting, task,
                    ActionListener.notifyOnce(new ActionListener<ShardOperationResult>() {

                        @Override
                        public void onResponse(ShardOperationResult shardOperationResult) {
                            shardResultOrExceptions.setOnce(finalShardIndex, shardOperationResult);
                            if (counter.decrementAndGet() == 0) {
                                finishHim(request, channel, task, shardResultOrExceptions);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            shardResultOrExceptions.setOnce(finalShardIndex, e);
                            if (counter.decrementAndGet() == 0) {
                                finishHim(request, channel, task, shardResultOrExceptions);
                            }
                        }
                    }));
            }
        }

        @SuppressWarnings("unchecked")
        private void finishHim(NodeRequest request, TransportChannel channel, Task task,
                               AtomicArray<Object> shardResultOrExceptions) {
            if (task instanceof CancellableTask) {
                try {
                    ((CancellableTask) task).ensureNotCancelled();
                } catch (TaskCancelledException e) {
                    try {
                        channel.sendResponse(e);
                    } catch (IOException ioException) {
                        e.addSuppressed(ioException);
                        logger.warn("failed to send response", e);
                    }
                    return;
                }
            }
            List<BroadcastShardOperationFailedException> accumulatedExceptions = new ArrayList<>();
            List<ShardOperationResult> results = new ArrayList<>();
            for (int i = 0; i < shardResultOrExceptions.length(); i++) {
                if (shardResultOrExceptions.get(i) instanceof BroadcastShardOperationFailedException) {
                    accumulatedExceptions.add((BroadcastShardOperationFailedException) shardResultOrExceptions.get(i));
                } else {
                    results.add((ShardOperationResult) shardResultOrExceptions.get(i));
                }
            }

            try {
                channel.sendResponse(new NodeResponse(request.getNodeId(), shardResultOrExceptions.length(), results,
                    accumulatedExceptions));
            } catch (IOException e) {
                logger.warn("failed to send response", e);
            }
        }

        private void onShardOperation(final NodeRequest request, final ShardRouting shardRouting, final Task task,
                                      final ActionListener<ShardOperationResult> listener) {
            if (task instanceof CancellableTask && ((CancellableTask)task).notifyIfCancelled(listener)) {
                return;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("[{}]  executing operation for shard [{}]", actionName, shardRouting.shortSummary());
            }
            final Consumer<Exception> failureHandler = e -> {
                BroadcastShardOperationFailedException failure =
                    new BroadcastShardOperationFailedException(shardRouting.shardId(), "operation " + actionName + " failed", e);
                failure.setShard(shardRouting.shardId());
                if (TransportActions.isShardNotAvailableException(e)) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(new ParameterizedMessage(
                            "[{}] failed to execute operation for shard [{}]", actionName, shardRouting.shortSummary()), e);
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(new ParameterizedMessage(
                            "[{}] failed to execute operation for shard [{}]", actionName, shardRouting.shortSummary()), e);
                    }
                }
                listener.onFailure(failure);
            };
            try {
                shardOperation(request.indicesLevelRequest, shardRouting, task, new ActionListener<ShardOperationResult>() {
                    @Override
                    public void onResponse(ShardOperationResult shardOperationResult) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("[{}]  completed operation for shard [{}]", actionName, shardRouting.shortSummary());
                        }
                        listener.onResponse(shardOperationResult);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        failureHandler.accept(e);
                    }
                });
            } catch (Exception e) {
                assert false : "shardOperation should not throw an exception, but delegate to listener instead";
                failureHandler.accept(e);
            }
        }
    }

    public class NodeRequest extends TransportRequest implements IndicesRequest {
        private String nodeId;

        private List<ShardRouting> shards;

        protected Request indicesLevelRequest;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            indicesLevelRequest = readRequestFrom(in);
            shards = in.readList(ShardRouting::new);
            nodeId = in.readString();
        }

        public NodeRequest(String nodeId, Request request, List<ShardRouting> shards) {
            this.indicesLevelRequest = request;
            this.shards = shards;
            this.nodeId = nodeId;
        }

        public List<ShardRouting> getShards() {
            return shards;
        }

        public String getNodeId() {
            return nodeId;
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

        NodeResponse(String nodeId,
                            int totalShards,
                            List<ShardOperationResult> results,
                            List<BroadcastShardOperationFailedException> exceptions) {
            this.nodeId = nodeId;
            this.totalShards = totalShards;
            this.results = results;
            this.exceptions = exceptions;
        }

        public String getNodeId() {
            return nodeId;
        }

        public int getTotalShards() {
            return totalShards;
        }

        public int getSuccessfulShards() {
            return results.size();
        }

        public List<BroadcastShardOperationFailedException> getExceptions() {
            return exceptions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeVInt(totalShards);
            out.writeVInt(results.size());
            for (ShardOperationResult result : results) {
                out.writeOptionalWriteable(result);
            }
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

        private EmptyResult(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) { }

        public static EmptyResult readEmptyResultFrom(StreamInput in) {
            return INSTANCE;
        }
    }
}
