/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.esql.heap_attack;

import org.elasticsearch.action.ActionType;
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
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportPauseFieldAction extends TransportNodesAction<
    TransportPauseFieldAction.Request,
    TransportPauseFieldAction.Response,
    TransportPauseFieldAction.NodeRequest,
    TransportPauseFieldAction.NodeResponse,
    Void> {

    public static final ActionType<Response> TYPE = new ActionType<>("cluster:monitor/test/pause_field");

    @Inject
    public TransportPauseFieldAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
    }

    @Override
    protected Response newResponse(Request request, List<NodeResponse> nodeResponses, List<FailedNodeException> failures) {
        return new Response(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request.block);
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in, node);
    }

    @Override
    protected NodeResponse nodeOperation(NodeRequest request, Task task) {
        if (request.block) {
            PausableField.blockExecution();
        } else {
            PausableField.unblockExecution();
        }
        return new NodeResponse(transportService.getLocalNode());
    }

    public static class Request extends BaseNodesRequest {
        private final boolean block;

        public Request(boolean block) {
            super(Strings.EMPTY_ARRAY);
            this.block = block;
        }
    }

    public static class NodeRequest extends AbstractTransportRequest {
        private final boolean block;

        NodeRequest(boolean block) {
            this.block = block;
        }

        NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.block = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(block);
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

    public static class NodeResponse extends BaseNodeResponse {
        protected NodeResponse(DiscoveryNode node) {
            super(node);
        }

        protected NodeResponse(StreamInput in) throws IOException {
            super(in);
        }

        protected NodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            super(in, node);
        }
    }
}
