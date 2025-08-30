/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportInferenceUsageAction extends XPackUsageFeatureTransportAction {

    private final Logger logger = LogManager.getLogger(TransportInferenceUsageAction.class);

    private final Client client;

    @Inject
    public TransportInferenceUsageAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client
    ) {
        super(XPackUsageFeatureAction.INFERENCE.name(), transportService, clusterService, threadPool, actionFilters);
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        GetInferenceModelAction.Request getInferenceModelAction = new GetInferenceModelAction.Request("_all", TaskType.ANY, false);
        client.execute(GetInferenceModelAction.INSTANCE, getInferenceModelAction, ActionListener.wrap(response -> {
            Map<String, InferenceFeatureSetUsage.ModelStats> stats = new TreeMap<>();
            List<ModelConfigurations> endpoints = response.getEndpoints();
            for (ModelConfigurations model : endpoints) {
                String statKey = model.getService() + ":" + model.getTaskType().name();
                InferenceFeatureSetUsage.ModelStats stat = stats.computeIfAbsent(
                    statKey,
                    key -> new InferenceFeatureSetUsage.ModelStats(model.getService(), model.getTaskType())
                );
                stat.add();
            }

            InferenceFeatureSetUsage usage = new InferenceFeatureSetUsage(
                stats.values(),
                getSemanticTextStats(state.getMetadata().indicesAllProjects(), endpoints)
            );
            listener.onResponse(new XPackUsageFeatureResponse(usage));
        }, e -> {
            logger.warn(Strings.format("Retrieving inference usage failed with error: %s", e.getMessage()), e);
            listener.onResponse(new XPackUsageFeatureResponse(InferenceFeatureSetUsage.EMPTY));
        }));
    }

    private static InferenceFeatureSetUsage.SemanticTextStats getSemanticTextStats(
        Iterable<IndexMetadata> indicesMetadata,
        List<ModelConfigurations> modelConfigurations
    ) {
        long fieldCount = 0;
        long indexCount = 0;

        Map<String, Long> inferenceIdsCounts = new HashMap<>();

        for (IndexMetadata indexMetadata : indicesMetadata) {
            Map<String, InferenceFieldMetadata> inferenceFields = indexMetadata.getInferenceFields();

            fieldCount += inferenceFields.size();
            indexCount += inferenceFields.isEmpty() ? 0 : 1;

            inferenceFields.forEach((fieldName, inferenceFieldMetadata) -> {
                String inferenceId = inferenceFieldMetadata.getInferenceId();
                inferenceIdsCounts.compute(inferenceId, (k, v) -> v == null ? 1 : v + 1);
            });
        }

        long sparseFieldsCount = 0;
        long denseFieldsCount = 0;
        long denseInferenceIdCount = 0;
        long sparseInferenceIdCount = 0;
        for (ModelConfigurations model : modelConfigurations) {
            String inferenceId = model.getInferenceEntityId();

            if (inferenceIdsCounts.containsKey(inferenceId) == false) {
                continue;
            }
            if (model.getTaskType() == TaskType.SPARSE_EMBEDDING) {
                sparseFieldsCount += inferenceIdsCounts.get(inferenceId);
                sparseInferenceIdCount += 1;
            } else {
                denseFieldsCount += inferenceIdsCounts.get(inferenceId);
                denseInferenceIdCount += 1;
            }
        }

        return new InferenceFeatureSetUsage.SemanticTextStats(
            fieldCount,
            indexCount,
            sparseFieldsCount,
            denseFieldsCount,
            denseInferenceIdCount,
            sparseInferenceIdCount
        );
    }
}
