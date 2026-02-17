/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.action;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionModel;
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsResponseHandler;
import org.elasticsearch.xpack.inference.services.nvidia.request.completion.NvidiaChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.nvidia.request.embeddings.NvidiaEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.nvidia.request.rerank.NvidiaRerankRequest;
import org.elasticsearch.xpack.inference.services.nvidia.rerank.NvidiaRerankModel;
import org.elasticsearch.xpack.inference.services.nvidia.rerank.NvidiaRerankResponseHandler;
import org.elasticsearch.xpack.inference.services.nvidia.response.rerank.NvidiaRerankResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiEmbeddingsResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

/**
 * Creates executable actions for Nvidia inference requests, handling embeddings, completions and reranking.
 * This class implements the {@link NvidiaActionVisitor} interface to provide specific action creation methods.
 */
public class NvidiaActionCreator implements NvidiaActionVisitor {

    private static final String FAILED_TO_SEND_REQUEST_ERROR_MESSAGE = "Failed to send Nvidia %s request from inference entity id [%s]";
    private static final String COMPLETION_ERROR_PREFIX = "Nvidia completions";
    private static final String USER_ROLE = "user";

    private static final ResponseHandler EMBEDDINGS_HANDLER = new NvidiaEmbeddingsResponseHandler(
        "Nvidia text embedding",
        OpenAiEmbeddingsResponseEntity::fromResponse
    );

    private static final ResponseHandler COMPLETION_HANDLER = new NvidiaCompletionResponseHandler(
        "Nvidia completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    private static final ResponseHandler RERANK_HANDLER = new NvidiaRerankResponseHandler(
        "Nvidia rerank",
        (request, response) -> NvidiaRerankResponseEntity.fromResponse(response)
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    /**
     * Constructs a new {@link NvidiaActionCreator} with the specified sender and service components.
     *
     * @param sender the sender to use for executing actions
     * @param serviceComponents the service components providing necessary services
     */
    public NvidiaActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(NvidiaEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = NvidiaEmbeddingsModel.of(model, taskSettings);
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            EMBEDDINGS_HANDLER,
            embeddingsInput -> new NvidiaEmbeddingsRequest(
                serviceComponents.truncator(),
                truncate(embeddingsInput.getTextInputs(), overriddenModel.getServiceSettings().maxInputTokens()),
                overriddenModel,
                embeddingsInput.getInputType()
            ),
            EmbeddingsInput.class
        );

        var errorMessage = buildErrorMessage(TaskType.TEXT_EMBEDDING, model.getInferenceEntityId());
        return new SenderExecutableAction(sender, manager, errorMessage);
    }

    @Override
    public ExecutableAction create(NvidiaChatCompletionModel model) {
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            COMPLETION_HANDLER,
            inputs -> new NvidiaChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), model),
            ChatCompletionInput.class
        );

        var errorMessage = buildErrorMessage(TaskType.COMPLETION, model.getInferenceEntityId());
        return new SingleInputSenderExecutableAction(sender, manager, errorMessage, COMPLETION_ERROR_PREFIX);
    }

    @Override
    public ExecutableAction create(NvidiaRerankModel model) {
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            RERANK_HANDLER,
            inputs -> new NvidiaRerankRequest(inputs.getQuery(), inputs.getChunks(), model),
            QueryAndDocsInputs.class
        );
        var errorMessage = buildErrorMessage(TaskType.RERANK, model.getInferenceEntityId());
        return new SenderExecutableAction(sender, manager, errorMessage);
    }

    /**
     * Builds an error message for failed requests.
     *
     * @param requestType the type of request that failed
     * @param inferenceId the inference entity ID associated with the request
     * @return a formatted error message
     */
    public static String buildErrorMessage(TaskType requestType, String inferenceId) {
        return format(FAILED_TO_SEND_REQUEST_ERROR_MESSAGE, requestType.toString(), inferenceId);
    }
}
