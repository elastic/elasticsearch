/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.capabilities;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.admin.cluster.RestNodesCapabilitiesAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class TransportNodesCapabilitiesAction extends TransportNodesAction<
    NodesCapabilitiesRequest,
    NodesCapabilitiesResponse,
    TransportNodesCapabilitiesAction.NodeCapabilitiesRequest,
    NodeCapability> {

    public static final ActionType<NodesCapabilitiesResponse> TYPE = new ActionType<>("cluster:monitor/nodes/capabilities");

    private final RestController restController;
    private final FeatureService featureService;

    @Inject
    public TransportNodesCapabilitiesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        RestController restController,
        FeatureService featureService
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            NodeCapabilitiesRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.restController = restController;
        this.featureService = featureService;
    }

    @Override
    protected void doExecute(Task task, NodesCapabilitiesRequest request, ActionListener<NodesCapabilitiesResponse> listener) {
        if (featureService.clusterHasFeature(clusterService.state(), RestNodesCapabilitiesAction.CAPABILITIES_ACTION) == false) {
            // not everything in the cluster supports capabilities.
            // Therefore we don't support whatever it is we're being asked for
            listener.onResponse(new NodesCapabilitiesResponse(clusterService.getClusterName(), List.of(), List.of()) {
                @Override
                public Optional<Boolean> isSupported() {
                    return Optional.of(false);
                }
            });
        } else {
            super.doExecute(task, request, listener);
        }
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
        return new NodeCapabilitiesRequest(
            request.method(),
            request.path(),
            request.parameters(),
            request.capabilities(),
            request.restApiVersion()
        );
    }

    @Override
    protected NodeCapability newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeCapability(in);
    }

    @Override
    protected NodeCapability nodeOperation(NodeCapabilitiesRequest request, Task task) {
        boolean supported = restController.checkSupported(
            request.method,
            request.path,
            request.parameters,
            request.capabilities,
            request.restApiVersion
        );
        return new NodeCapability(supported, transportService.getLocalNode());
    }

    public static class NodeCapabilitiesRequest extends TransportRequest {
        private final RestRequest.Method method;
        private final String path;
        private final Set<String> parameters;
        private final Set<String> capabilities;
        private final RestApiVersion restApiVersion;

        public NodeCapabilitiesRequest(StreamInput in) throws IOException {
            super(in);

            method = in.readEnum(RestRequest.Method.class);
            path = in.readString();
            parameters = in.readCollectionAsImmutableSet(StreamInput::readString);
            capabilities = in.readCollectionAsImmutableSet(StreamInput::readString);
            restApiVersion = RestApiVersion.forMajor(in.readVInt());
        }

        public NodeCapabilitiesRequest(
            RestRequest.Method method,
            String path,
            Set<String> parameters,
            Set<String> capabilities,
            RestApiVersion restApiVersion
        ) {
            this.method = method;
            this.path = path;
            this.parameters = Set.copyOf(parameters);
            this.capabilities = Set.copyOf(capabilities);
            this.restApiVersion = restApiVersion;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);

            out.writeEnum(method);
            out.writeString(path);
            out.writeCollection(parameters, StreamOutput::writeString);
            out.writeCollection(capabilities, StreamOutput::writeString);
            out.writeVInt(restApiVersion.major);
        }
    }
}
