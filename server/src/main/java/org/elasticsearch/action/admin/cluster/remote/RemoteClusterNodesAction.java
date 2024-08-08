/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.remote;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoMetrics;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterServerInfo;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class RemoteClusterNodesAction {

    public static final String NAME = "cluster:internal/remote_cluster/nodes";
    public static final ActionType<RemoteClusterNodesAction.Response> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<Response> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        RemoteClusterNodesAction.Response::new
    );

    public static class Request extends ActionRequest {
        public static final Request ALL_NODES = new Request(false);
        public static final Request REMOTE_CLUSTER_SERVER_NODES = new Request(true);
        private final boolean remoteClusterServer;

        private Request(boolean remoteClusterServer) {
            this.remoteClusterServer = remoteClusterServer;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.remoteClusterServer = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(remoteClusterServer);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {

        private final List<DiscoveryNode> nodes;

        public Response(List<DiscoveryNode> nodes) {
            this.nodes = nodes;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.nodes = in.readCollectionAsList(DiscoveryNode::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(nodes);
        }

        public List<DiscoveryNode> getNodes() {
            return nodes;
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {
        private final Client client;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters, Client client) {
            super(TYPE.name(), transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            this.client = client;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            final ThreadContext threadContext = client.threadPool().getThreadContext();
            executeWithSystemContext(
                request,
                threadContext,
                ContextPreservingActionListener.wrapPreservingContext(listener, threadContext)
            );
        }

        private void executeWithSystemContext(Request request, ThreadContext threadContext, ActionListener<Response> listener) {
            try (var ignore = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                if (request.remoteClusterServer) {
                    final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().clear()
                        .addMetrics(NodesInfoMetrics.Metric.REMOTE_CLUSTER_SERVER.metricName());
                    client.execute(TransportNodesInfoAction.TYPE, nodesInfoRequest, listener.delegateFailureAndWrap((l, response) -> {
                        l.onResponse(new Response(response.getNodes().stream().map(nodeInfo -> {
                            final RemoteClusterServerInfo remoteClusterServerInfo = nodeInfo.getInfo(RemoteClusterServerInfo.class);
                            if (remoteClusterServerInfo == null) {
                                return null;
                            }
                            return nodeInfo.getNode().withTransportAddress(remoteClusterServerInfo.getAddress().publishAddress());
                        }).filter(Objects::nonNull).toList()));
                    }));
                } else {
                    final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().clear();
                    client.execute(
                        TransportNodesInfoAction.TYPE,
                        nodesInfoRequest,
                        listener.delegateFailureAndWrap(
                            (l, response) -> l.onResponse(
                                new Response(response.getNodes().stream().map(BaseNodeResponse::getNode).toList())
                            )
                        )
                    );
                }
            }
        }
    }
}
