/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Broadcasts a {@link Writeable} to all nodes and responds with an empty object.
 * This is intended to be used as a fire-and-forget style, where responses and failures are logged and swallowed.
 */
public abstract class BroadcastMessageAction<Message extends Writeable> extends TransportNodesAction<
    BroadcastMessageAction.Request<Message>,
    BroadcastMessageAction.Response,
    BroadcastMessageAction.NodeRequest<Message>,
    BroadcastMessageAction.NodeResponse,
    Void> {

    protected BroadcastMessageAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Message> messageReader
    ) {
        super(
            actionName,
            clusterService,
            transportService,
            actionFilters,
            in -> new NodeRequest<>(messageReader.read(in)),
            clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
    }

    @Override
    protected Response newResponse(Request<Message> request, List<NodeResponse> nodeResponses, List<FailedNodeException> failures) {
        return new Response(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeRequest<Message> newNodeRequest(Request<Message> request) {
        return new NodeRequest<>(request.message);
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in, node);
    }

    @Override
    protected NodeResponse nodeOperation(NodeRequest<Message> request, Task task) {
        receiveMessage(request.message);
        return new NodeResponse(transportService.getLocalNode());
    }

    /**
     * This method is run on each node in the cluster.
     */
    protected abstract void receiveMessage(Message message);

    public static <T extends Writeable> Request<T> request(T message, TimeValue timeout) {
        return new Request<>(message, timeout);
    }

    public static class Request<Message extends Writeable> extends BaseNodesRequest {
        private final Message message;

        protected Request(Message message, TimeValue timeout) {
            super(Strings.EMPTY_ARRAY);
            this.message = message;
            setTimeout(timeout);
        }
    }

    public static class Response extends BaseNodesResponse<NodeResponse> {

        protected Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) {
            TransportAction.localOnly();
        }
    }

    public static class NodeRequest<Message extends Writeable> extends AbstractTransportRequest {
        private final Message message;

        private NodeRequest(Message message) {
            this.message = message;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "broadcasted message to an individual node", parentTaskId, headers);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {
        protected NodeResponse(StreamInput in) throws IOException {
            super(in);
        }

        protected NodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            super(in, node);
        }

        protected NodeResponse(DiscoveryNode node) {
            super(node);
        }
    }
}
