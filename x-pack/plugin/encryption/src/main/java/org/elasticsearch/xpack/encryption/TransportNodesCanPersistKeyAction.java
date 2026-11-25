/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Asks nodes whether they can persist the project encryption key with their cluster state. The elected master installs or restores the
 * key only once every node that persists cluster state can, since a node that cannot persist the key cannot persist cluster state at all.
 */
public class TransportNodesCanPersistKeyAction extends TransportNodesAction<
    TransportNodesCanPersistKeyAction.Request,
    TransportNodesCanPersistKeyAction.Response,
    TransportNodesCanPersistKeyAction.NodeRequest,
    TransportNodesCanPersistKeyAction.NodeResponse,
    Void> {

    public static final ActionType<Response> TYPE = new ActionType<>("internal:cluster/encryption/key/can_persist");

    private final PasswordPekEncryption pekEncryption;

    @Inject
    public TransportNodesCanPersistKeyAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        PasswordPekEncryption pekEncryption
    ) {
        super(TYPE.name(), clusterService, transportService, actionFilters, NodeRequest::new, transportService.getThreadPool().generic());
        this.pekEncryption = pekEncryption;
    }

    @Override
    protected Response newResponse(Request request, List<NodeResponse> responses, List<FailedNodeException> failures) {
        return new Response(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest();
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in);
    }

    @Override
    protected NodeResponse nodeOperation(NodeRequest request, Task task) {
        return new NodeResponse(transportService.getLocalNode(), pekEncryption.canWrapForGateway());
    }

    public static class Request extends BaseNodesRequest {
        // for tests
        private final List<DiscoveryNode> nodes;

        public Request(DiscoveryNode... nodes) {
            super(nodes);
            this.nodes = List.of(nodes);
        }

        List<DiscoveryNode> nodes() {
            return nodes;
        }
    }

    public static class Response extends BaseNodesResponse<NodeResponse> {
        Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        /** Ephemeral ids, so that a node that restarts after answering, possibly without its password, is no longer counted as ready. */
        Set<String> readyNodeIds() {
            return getNodes().stream()
                .filter(NodeResponse::canPersist)
                .map(r -> r.getNode().getEphemeralId())
                .collect(Collectors.toUnmodifiableSet());
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) {
            return TransportAction.localOnly();
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) {
            TransportAction.localOnly();
        }
    }

    public static class NodeRequest extends AbstractTransportRequest {
        NodeRequest() {}

        NodeRequest(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {
        private final boolean canPersist;

        NodeResponse(DiscoveryNode node, boolean canPersist) {
            super(node);
            this.canPersist = canPersist;
        }

        NodeResponse(StreamInput in) throws IOException {
            super(in);
            this.canPersist = in.readBoolean();
        }

        boolean canPersist() {
            return canPersist;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(canPersist);
        }
    }
}
