/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.querysampling.QuerySamplingService;

import java.io.IOException;
import java.util.List;

/**
 * Runs on every selected node, which computes the ground truth of its own sampled queries. The exact searches
 * are issued through the node client while the thread context still holds the caller, so they see what the
 * caller is allowed to see and nothing more.
 */
public final class TransportQuerySamplingGroundTruthAction extends TransportNodesAction<
    QuerySamplingGroundTruthRequest,
    QuerySamplingGroundTruthResponse,
    TransportQuerySamplingGroundTruthAction.NodeRequest,
    QuerySamplingNodeGroundTruthResponse,
    Void> {

    private final QuerySamplingService querySamplingService;
    private final Client client;

    @Inject
    public TransportQuerySamplingGroundTruthAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        QuerySamplingService querySamplingService,
        Client client
    ) {
        super(
            QuerySamplingGroundTruthAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.querySamplingService = querySamplingService;
        this.client = client;
    }

    @Override
    protected QuerySamplingGroundTruthResponse newResponse(
        QuerySamplingGroundTruthRequest request,
        List<QuerySamplingNodeGroundTruthResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new QuerySamplingGroundTruthResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(QuerySamplingGroundTruthRequest request) {
        return new NodeRequest(request.max());
    }

    @Override
    protected QuerySamplingNodeGroundTruthResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new QuerySamplingNodeGroundTruthResponse(in);
    }

    @Override
    protected QuerySamplingNodeGroundTruthResponse nodeOperation(NodeRequest request, Task task) {
        throw new UnsupportedOperationException("the searches are asynchronous, see nodeOperationAsync");
    }

    @Override
    protected void nodeOperationAsync(NodeRequest request, Task task, ActionListener<QuerySamplingNodeGroundTruthResponse> listener) {
        querySamplingService.computeGroundTruth(
            request.max,
            client::search,
            listener.map(result -> new QuerySamplingNodeGroundTruthResponse(clusterService.localNode(), result.computed(), result.failed()))
        );
    }

    static final class NodeRequest extends AbstractTransportRequest {

        private final int max;

        NodeRequest(int max) {
            this.max = max;
        }

        NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.max = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(max);
        }
    }
}
