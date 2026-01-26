/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

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

import static org.elasticsearch.action.admin.indices.sampling.PostSampleDocsAction.NodeRequest;
import static org.elasticsearch.action.admin.indices.sampling.PostSampleDocsAction.NodeResponse;
import static org.elasticsearch.action.admin.indices.sampling.PostSampleDocsAction.Request;
import static org.elasticsearch.action.admin.indices.sampling.PostSampleDocsAction.Response;

/**
 * Transport action for manually adding documents to samples.
 * <p>
 * This action distributes the request to all nodes in the cluster, where each node
 * adds the documents to its local sample storage.
 * </p>
 */
public class TransportPostSampleDocsAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse, Void> {
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SamplingService samplingService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportPostSampleDocsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SamplingService samplingService,
        ProjectResolver projectResolver
    ) {
        super(
            PostSampleDocsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.samplingService = samplingService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected Void createActionContext(Task task, Request request) {
        // Validate that the index or data stream exists
        SamplingService.throwIndexNotFoundExceptionIfNotDataStreamOrIndex(
            indexNameExpressionResolver,
            projectResolver,
            clusterService.state(),
            request
        );

        // Note: We no longer require a sampling configuration to exist.
        // If one doesn't exist, addSampleDocuments will use default values
        // (rate=1.0, maxSamples=100, no condition).

        return null;
    }

    @Override
    protected Response newResponse(Request request, List<NodeResponse> nodeResponses, List<FailedNodeException> failures) {
        return new Response(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request.getIndexName(), request.getDocuments());
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in);
    }

    @Override
    protected NodeResponse nodeOperation(NodeRequest request, Task task) {
        String indexName = request.getIndexName();
        List<java.util.Map<String, Object>> documents = request.getDocuments();

        // Add the documents to the local sample
        SamplingService.AddSampleDocsResult result = samplingService.addSampleDocuments(
            projectResolver.getProjectId(),
            indexName,
            documents
        );

        return new NodeResponse(transportService.getLocalNode(), result.getAdded(), result.getRejected(), result.getFailures());
    }
}
