/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.ResetMlComponentsAction;
import org.elasticsearch.xpack.ml.MachineLearningFeatures;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.io.IOException;
import java.util.List;

public class TransportResetMlComponentsAction extends TransportNodesAction<
    ResetMlComponentsAction.Request,
    ResetMlComponentsAction.Response,
    ResetMlComponentsAction.NodeRequest,
    ResetMlComponentsAction.Response.ResetResponse,
    Void> {

    private final AnomalyDetectionAuditor anomalyDetectionAuditor;
    private final DataFrameAnalyticsAuditor dfaAuditor;
    private final InferenceAuditor inferenceAuditor;
    private final TrainedModelStatsService trainedModelStatsService;
    private final FeatureService featureService;

    @Inject
    public TransportResetMlComponentsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        AnomalyDetectionAuditor anomalyDetectionAuditor,
        DataFrameAnalyticsAuditor dfaAuditor,
        InferenceAuditor inferenceAuditor,
        TrainedModelStatsService trainedModelStatsService,
        FeatureService featureService
    ) {
        super(
            ResetMlComponentsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            ResetMlComponentsAction.NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.anomalyDetectionAuditor = anomalyDetectionAuditor;
        this.dfaAuditor = dfaAuditor;
        this.inferenceAuditor = inferenceAuditor;
        this.trainedModelStatsService = trainedModelStatsService;
        this.featureService = featureService;
    }

    @Override
    protected void doExecute(
        Task task,
        ResetMlComponentsAction.Request request,
        ActionListener<ResetMlComponentsAction.Response> listener
    ) {
        if (featureService.clusterHasFeature(clusterService.state(), MachineLearningFeatures.COMPONENTS_RESET_ACTION) == false) {
            listener.onResponse(new ResetMlComponentsAction.Response(clusterService.getClusterName(), List.of(), List.of()));
        } else {
            super.doExecute(task, request, listener);
        }
    }

    @Override
    protected ResetMlComponentsAction.Response newResponse(
        ResetMlComponentsAction.Request request,
        List<ResetMlComponentsAction.Response.ResetResponse> resetResponses,
        List<FailedNodeException> failures
    ) {
        return new ResetMlComponentsAction.Response(clusterService.getClusterName(), resetResponses, failures);
    }

    @Override
    protected ResetMlComponentsAction.NodeRequest newNodeRequest(ResetMlComponentsAction.Request request) {
        return new ResetMlComponentsAction.NodeRequest();
    }

    @Override
    protected ResetMlComponentsAction.Response.ResetResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new ResetMlComponentsAction.Response.ResetResponse(in);
    }

    @Override
    protected ResetMlComponentsAction.Response.ResetResponse nodeOperation(ResetMlComponentsAction.NodeRequest request, Task task) {
        anomalyDetectionAuditor.reset();
        dfaAuditor.reset();
        inferenceAuditor.reset();
        trainedModelStatsService.clearQueue();
        return new ResetMlComponentsAction.Response.ResetResponse(clusterService.localNode(), true);
    }
}
