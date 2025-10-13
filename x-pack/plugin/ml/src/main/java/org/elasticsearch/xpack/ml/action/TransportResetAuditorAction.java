/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.ResetAuditorAction;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.io.IOException;
import java.util.List;

public class TransportResetAuditorAction extends TransportNodesAction<
    ResetAuditorAction.Request,
    ResetAuditorAction.Response,
    ResetAuditorAction.NodeRequest,
    ResetAuditorAction.Response.ResetResponse,
    Void> {

    private final AnomalyDetectionAuditor anomalyDetectionAuditor;
    private final DataFrameAnalyticsAuditor dfaAuditor;
    private final InferenceAuditor inferenceAuditor;

    @Inject
    public TransportResetAuditorAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        AnomalyDetectionAuditor anomalyDetectionAuditor,
        DataFrameAnalyticsAuditor dfaAuditor,
        InferenceAuditor inferenceAuditor
    ) {
        super(
            ResetAuditorAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            ResetAuditorAction.NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.anomalyDetectionAuditor = anomalyDetectionAuditor;
        this.dfaAuditor = dfaAuditor;
        this.inferenceAuditor = inferenceAuditor;
    }

    @Override
    protected ResetAuditorAction.Response newResponse(
        ResetAuditorAction.Request request,
        List<ResetAuditorAction.Response.ResetResponse> resetResponses,
        List<FailedNodeException> failures
    ) {
        return new ResetAuditorAction.Response(clusterService.getClusterName(), resetResponses, failures);
    }

    @Override
    protected ResetAuditorAction.NodeRequest newNodeRequest(ResetAuditorAction.Request request) {
        return new ResetAuditorAction.NodeRequest();
    }

    @Override
    protected ResetAuditorAction.Response.ResetResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new ResetAuditorAction.Response.ResetResponse(in);
    }

    @Override
    protected ResetAuditorAction.Response.ResetResponse nodeOperation(ResetAuditorAction.NodeRequest request, Task task) {
        anomalyDetectionAuditor.reset();
        dfaAuditor.reset();
        inferenceAuditor.reset();
        return new ResetAuditorAction.Response.ResetResponse(clusterService.localNode(), true);
    }
}
