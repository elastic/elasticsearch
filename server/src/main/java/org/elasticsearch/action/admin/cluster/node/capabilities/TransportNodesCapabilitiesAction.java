/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.capabilities;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TransportNodesCapabilitiesAction extends TransportNodesAction<
    NodesCapabilitiesRequest,
    NodesCapabilitiesResponse,
    TransportNodesCapabilitiesAction.NodeCapabilitiesRequest,
    NodeCapability> {

    public static final ActionType<NodesCapabilitiesResponse> TYPE = new ActionType<>("cluster:monitor/nodes/capabilities");

    @Inject
    public TransportNodesCapabilitiesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            NodeCapabilitiesRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
    }

    @Override
    protected NodesCapabilitiesResponse newResponse(
        NodesCapabilitiesRequest request,
        List<NodeCapability> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesCapabilitiesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeCapabilitiesRequest newNodeRequest(NodesCapabilitiesRequest request) {
        return new NodeCapabilitiesRequest(request.method(), request.path(), request.parameters(), request.features());
    }

    @Override
    protected NodeCapability newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeCapability(in);
    }

    @Override
    protected NodeCapability nodeOperation(NodeCapabilitiesRequest request, Task task) {

    }

    public static class NodeCapabilitiesRequest extends TransportRequest {
        private final RestRequest.Method method;
        private final String path;
        private final Set<String> parameters;
        private final Set<String> features;

        public NodeCapabilitiesRequest(StreamInput in) throws IOException {
            super(in);

            method = in.readEnum(RestRequest.Method.class);
            path = in.readString();
            parameters = in.readCollectionAsImmutableSet(StreamInput::readString);
            features = in.readCollectionAsImmutableSet(StreamInput::readString);
        }

        public NodeCapabilitiesRequest(RestRequest.Method method, String path, Set<String> parameters, Set<String> features) {
            this.method = method;
            this.path = path;
            this.parameters = Set.copyOf(parameters);
            this.features = Set.copyOf(features);
        }

        public RestRequest.Method getMethod() {
            return method;
        }

        public String path() {
            return path;
        }

        public Set<String> parameters() {
            return parameters;
        }

        public Set<String> features() {
            return features;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);

            out.writeEnum(method);
            out.writeString(path);
            out.writeCollection(parameters, StreamOutput::writeString);
            out.writeCollection(features, StreamOutput::writeString);
        }
    }
}
