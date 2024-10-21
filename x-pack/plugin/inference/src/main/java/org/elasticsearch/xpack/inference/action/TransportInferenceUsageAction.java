/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.inference.InferenceFeatureSetUsage;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;

import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportInferenceUsageAction extends XPackUsageFeatureTransportAction {

    private final Client client;

    @Inject
    public TransportInferenceUsageAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            XPackUsageFeatureAction.INFERENCE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        GetInferenceModelAction.Request getInferenceModelAction = new GetInferenceModelAction.Request("_all", TaskType.ANY, false);
        client.execute(GetInferenceModelAction.INSTANCE, getInferenceModelAction, listener.delegateFailureAndWrap((delegate, response) -> {
            Map<String, InferenceFeatureSetUsage.ModelStats> stats = new TreeMap<>();
            for (ModelConfigurations model : response.getEndpoints()) {
                String statKey = model.getService() + ":" + model.getTaskType().name();
                InferenceFeatureSetUsage.ModelStats stat = stats.computeIfAbsent(
                    statKey,
                    key -> new InferenceFeatureSetUsage.ModelStats(model.getService(), model.getTaskType())
                );
                stat.add();
            }
            InferenceFeatureSetUsage usage = new InferenceFeatureSetUsage(stats.values());
            delegate.onResponse(new XPackUsageFeatureResponse(usage));
        }));
    }
}
