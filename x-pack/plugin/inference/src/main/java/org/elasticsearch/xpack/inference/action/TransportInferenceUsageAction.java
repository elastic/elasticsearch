/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.inference.InferenceFeatureSetUsage;

import java.util.Map;

public class TransportInferenceUsageAction extends XPackUsageFeatureTransportAction {

    @Inject
    public TransportInferenceUsageAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(XPackUsageFeatureAction.INFERENCE.name(), transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver);
    }

    @Override
    protected void masterOperation(
        Task task, XPackUsageRequest request, ClusterState state, ActionListener<XPackUsageFeatureResponse> listener
    ) throws Exception {
        Map<String, Object> stats = Map.of("my-model-id", Map.of(TaskType.SPARSE_EMBEDDING.name(), 25L));
        InferenceFeatureSetUsage usage = new InferenceFeatureSetUsage(true, true, stats);
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }
}
