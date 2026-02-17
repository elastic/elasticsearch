/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.action.admin.indices.sampling.GetSampleStatsAction.NodeRequest;
import static org.elasticsearch.action.admin.indices.sampling.GetSampleStatsAction.NodeResponse;
import static org.elasticsearch.action.admin.indices.sampling.GetSampleStatsAction.Request;
import static org.elasticsearch.action.admin.indices.sampling.GetSampleStatsAction.Response;

public class TransportGetSampleStatsAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse, Void> {
    private final SamplingService samplingService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportGetSampleStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        SamplingService samplingService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSampleStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.samplingService = samplingService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected Void createActionContext(Task task, GetSampleStatsAction.Request request) {
        String indexName = request.indices()[0];
        SamplingConfiguration samplingConfiguration = samplingService.getSamplingConfiguration(
            projectResolver.getProjectMetadata(clusterService.state()),
            request.indices()[0]
        );
        if (samplingConfiguration == null) {
            throw new ResourceNotFoundException("No sampling configuration found for [" + indexName + "]");
        }
        return null;
    }

    @Override
    protected Response newResponse(Request request, List<NodeResponse> nodeResponses, List<FailedNodeException> failures) {
        SamplingService.throwIndexNotFoundExceptionIfNotDataStreamOrIndex(
            indexNameExpressionResolver,
            projectResolver,
            clusterService.state(),
            request
        );
        SamplingConfiguration samplingConfiguration = samplingService.getSamplingConfiguration(
            projectResolver.getProjectMetadata(clusterService.state()),
            request.indices()[0]
        );
        int maxSamples = samplingConfiguration == null ? 0 : samplingConfiguration.maxSamples();
        return new Response(clusterService.getClusterName(), nodeResponses, failures, maxSamples);
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
        SamplingService.SampleStats sampleStats = samplingService.getLocalSampleStats(
            projectResolver.getProjectId(),
            request.getIndexName()
        );
        return new NodeResponse(transportService.getLocalNode(), sampleStats);
    }
}
