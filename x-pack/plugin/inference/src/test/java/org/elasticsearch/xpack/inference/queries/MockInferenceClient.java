/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.inference.model.TestModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

public class MockInferenceClient extends NoOpClient {
    private final Map<String, MinimalServiceSettings> inferenceEndpoints;
    private final MockInferenceGenerator inferenceGenerator;
    private final Map<String, MockInferenceRemoteClusterClient> remoteClusterClients;

    public MockInferenceClient(
        ThreadPool threadPool,
        Map<String, MinimalServiceSettings> inferenceEndpoints,
        Map<String, MockInferenceRemoteClusterClient.RemoteClusterConfig> remoteClusterConfigs
    ) {
        super(threadPool);
        this.inferenceEndpoints = inferenceEndpoints;
        this.inferenceGenerator = new MockInferenceGenerator(inferenceEndpoints);
        this.remoteClusterClients = generateRemoteClusterClients(remoteClusterConfigs);
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        if (action instanceof GetInferenceModelAction && request instanceof GetInferenceModelAction.Request getModelRequest) {
            @SuppressWarnings("unchecked")
            ActionListener<GetInferenceModelAction.Response> getModelListener = (ActionListener<GetInferenceModelAction.Response>) listener;
            String inferenceId = getModelRequest.getInferenceEntityId();
            MinimalServiceSettings settings = inferenceEndpoints.get(inferenceId);
            if (settings == null) {
                getModelListener.onFailure(new IllegalArgumentException("Inference endpoint [" + inferenceId + "] does not exist"));
            } else {
                ModelConfigurations modelConfig = new ModelConfigurations(
                    inferenceId,
                    settings.taskType(),
                    "mock-service",
                    new TestModel.TestServiceSettings("mock", settings.dimensions(), settings.similarity(), settings.elementType())
                );
                getModelListener.onResponse(new GetInferenceModelAction.Response(List.of(modelConfig)));
            }
        } else if (action instanceof EmbeddingAction && request instanceof EmbeddingAction.Request embeddingRequest) {
            @SuppressWarnings("unchecked")
            ActionListener<InferenceAction.Response> inferenceListener = (ActionListener<InferenceAction.Response>) listener;
            String inferenceId = embeddingRequest.getInferenceEntityId();
            MinimalServiceSettings settings = inferenceEndpoints.get(inferenceId);
            if (settings != null && settings.taskType().isAnyOrSame(TaskType.EMBEDDING) == false) {
                inferenceListener.onFailure(
                    new IllegalArgumentException(
                        "Inference endpoint ["
                            + inferenceId
                            + "] with task type ["
                            + settings.taskType()
                            + "] does not support EmbeddingAction; expected task type [embedding]"
                    )
                );
                return;
            }
            String input = embeddingRequest.getEmbeddingRequest().inputs().getFirst().textValue();
            executeInferenceAction(inferenceId, input, inferenceListener);
        } else if (action instanceof InferenceAction && request instanceof InferenceAction.Request inferenceRequest) {
            @SuppressWarnings("unchecked")
            ActionListener<InferenceAction.Response> inferenceListener = (ActionListener<InferenceAction.Response>) listener;
            String inferenceId = inferenceRequest.getInferenceEntityId();
            MinimalServiceSettings settings = inferenceEndpoints.get(inferenceId);
            if (settings != null && settings.taskType() != TaskType.SPARSE_EMBEDDING && settings.taskType() != TaskType.TEXT_EMBEDDING) {
                inferenceListener.onFailure(
                    new IllegalArgumentException(
                        "Inference endpoint ["
                            + inferenceId
                            + "] with task type ["
                            + settings.taskType()
                            + "] does not support InferenceAction; expected task type [sparse_embedding] or [text_embedding]"
                    )
                );
                return;
            }
            executeInferenceAction(inferenceId, inferenceRequest.getInput().getFirst(), inferenceListener);
        } else if (action instanceof CoordinatedInferenceAction && request instanceof CoordinatedInferenceAction.Request inferenceRequest) {
            @SuppressWarnings("unchecked")
            ActionListener<InferModelAction.Response> inferenceListener = (ActionListener<InferModelAction.Response>) listener;

            String inferenceId = inferenceRequest.getModelId();
            MinimalServiceSettings settings = inferenceEndpoints.get(inferenceId);
            if (settings != null && settings.taskType() != TaskType.TEXT_EMBEDDING && settings.taskType() != TaskType.SPARSE_EMBEDDING) {
                inferenceListener.onFailure(
                    new IllegalArgumentException(
                        "Inference endpoint ["
                            + inferenceId
                            + "] with task type ["
                            + settings.taskType()
                            + "] does not support CoordinatedInferenceAction; expected task type [text_embedding] or [sparse_embedding]"
                    )
                );
                return;
            }
            String input = inferenceRequest.getInputs().getFirst();

            try {
                InferenceResults inferenceResults = inferenceGenerator.generate(inferenceId, input);
                inferenceListener.onResponse(new InferModelAction.Response(List.of(inferenceResults), inferenceId, true));
            } catch (Exception e) {
                inferenceListener.onFailure(e);
            }
        } else {
            super.doExecute(action, request, listener);
        }
    }

    @Override
    public RemoteClusterClient getRemoteClusterClient(
        String clusterAlias,
        Executor responseExecutor,
        RemoteClusterService.DisconnectedStrategy disconnectedStrategy
    ) {
        RemoteClusterClient remoteClusterClient = remoteClusterClients.get(clusterAlias);
        if (remoteClusterClient == null) {
            throw new NoSuchRemoteClusterException(clusterAlias);
        }

        return remoteClusterClient;
    }

    private void executeInferenceAction(String inferenceId, String input, ActionListener<InferenceAction.Response> listener) {
        try {
            listener.onResponse(new InferenceAction.Response(inferenceGenerator.generateServiceResults(inferenceId, input)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static Map<String, MockInferenceRemoteClusterClient> generateRemoteClusterClients(
        Map<String, MockInferenceRemoteClusterClient.RemoteClusterConfig> remoteClusterConfigs
    ) {
        Map<String, MockInferenceRemoteClusterClient> remoteClusterClients = new HashMap<>();
        remoteClusterConfigs.forEach((clusterAlias, remoteClusterConfig) -> {
            MockInferenceRemoteClusterClient remoteClusterClient = new MockInferenceRemoteClusterClient(remoteClusterConfig);
            remoteClusterClients.put(clusterAlias, remoteClusterClient);
        });

        return remoteClusterClients;
    }
}
