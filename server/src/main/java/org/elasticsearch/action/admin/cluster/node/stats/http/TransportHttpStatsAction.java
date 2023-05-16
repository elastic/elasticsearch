/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats.http;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TransportHttpStatsAction extends TransportNodesAction<
    TransportHttpStatsAction.HttpStatsRequest,
    HttpStatsNodesResponse, // response that the caller will get
    TransportHttpStatsAction.HttpStatsTransportRequest,
    HttpStatsContent> { // object shared between nodes

    private final NodeService nodeService;

    @Inject
    public TransportHttpStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        NodeService nodeService,
        ActionFilters actionFilters
    ) {
        super(
            HttpStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            HttpStatsRequest::new,
            HttpStatsTransportRequest::new,
            ThreadPool.Names.MANAGEMENT,
            HttpStatsContent.class
        );
        this.nodeService = nodeService;
    }

    @Override
    protected HttpStatsNodesResponse newResponse(HttpStatsRequest request, List<HttpStatsContent> httpStatsContents, List<FailedNodeException> failures) {
        return new HttpStatsNodesResponse(clusterService.getClusterName(), httpStatsContents, failures);
    }

    @Override
    protected HttpStatsTransportRequest newNodeRequest(HttpStatsRequest request) {
        return new HttpStatsTransportRequest(request);
    }

    @Override
    protected HttpStatsContent newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return null;
    }

    @Override
    protected HttpStatsContent nodeOperation(HttpStatsTransportRequest request, Task task) {
        assert task instanceof CancellableTask;

        return nodeService.httpStats();
    }

    public static class HttpStatsTransportRequest extends TransportRequest {

        HttpStatsRequest request;

        public HttpStatsTransportRequest(StreamInput in) throws IOException {
            super(in);
            request = new HttpStatsRequest(in);
        }

        public HttpStatsTransportRequest(HttpStatsRequest request) {
            this.request = request;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }


    public static class HttpStatsRequest extends BaseNodesRequest<HttpStatsRequest> {

        public HttpStatsRequest(StreamInput in) throws IOException {
            super(in);
        }

        public HttpStatsRequest(String... nodesIds) {
            super(nodesIds);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
