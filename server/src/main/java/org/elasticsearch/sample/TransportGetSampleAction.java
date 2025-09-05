/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sample;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.sample.GetSampleAction.NodeRequest;
import static org.elasticsearch.sample.GetSampleAction.NodeResponse;
import static org.elasticsearch.sample.GetSampleAction.Request;
import static org.elasticsearch.sample.GetSampleAction.Response;

public class TransportGetSampleAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse, Void> {
    private final SamplingService samplingService;

    @Inject
    public TransportGetSampleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        SamplingService samplingService
    ) {
        super(
            GetSampleAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.samplingService = samplingService;
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    protected Response newResponse(Request request, List<NodeResponse> nodeResponses, List<FailedNodeException> failures) {
        return new Response(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request.indices()[0]);
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in);
    }

    @Override
    protected NodeResponse nodeOperation(NodeRequest request, Task task) {
        String index = request.getIndex();
        List<IndexRequest> samples = samplingService.getSamples(index);
        return new NodeResponse(transportService.getLocalNode(), samples == null ? List.of() : samples);
    }
}
