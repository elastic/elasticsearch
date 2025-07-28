/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.features;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

@UpdateForV10(owner = UpdateForV10.Owner.CORE_INFRA)
// this can be removed in v10. It may be called by v8 nodes to v9 nodes.
public class TransportNodesFeaturesAction extends TransportNodesAction<
    NodesFeaturesRequest,
    NodesFeaturesResponse,
    TransportNodesFeaturesAction.NodeFeaturesRequest,
    NodeFeatures,
    Void> {

    public static final ActionType<NodesFeaturesResponse> TYPE = new ActionType<>("cluster:monitor/nodes/features");

    private final FeatureService featureService;

    @Inject
    public TransportNodesFeaturesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        FeatureService featureService
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            NodeFeaturesRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.featureService = featureService;
    }

    @Override
    protected NodesFeaturesResponse newResponse(
        NodesFeaturesRequest request,
        List<NodeFeatures> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesFeaturesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeFeaturesRequest newNodeRequest(NodesFeaturesRequest request) {
        return new NodeFeaturesRequest();
    }

    @Override
    protected NodeFeatures newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeFeatures(in);
    }

    @Override
    protected NodeFeatures nodeOperation(NodeFeaturesRequest request, Task task) {
        return new NodeFeatures(featureService.getNodeFeatures().keySet(), transportService.getLocalNode());
    }

    public static class NodeFeaturesRequest extends AbstractTransportRequest {
        public NodeFeaturesRequest(StreamInput in) throws IOException {
            super(in);
        }

        public NodeFeaturesRequest() {}
    }
}
