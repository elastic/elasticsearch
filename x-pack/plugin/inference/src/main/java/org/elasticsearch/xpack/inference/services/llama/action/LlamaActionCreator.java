/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.action;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionModel;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsResponseHandler;
import org.elasticsearch.xpack.inference.services.llama.request.completion.LlamaChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.llama.request.embeddings.LlamaEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.llama.response.embeddings.LlamaEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;

import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

/**
 * Creates actions for Llama inference requests, handling both embeddings and completions.
 * This class implements the {@link LlamaActionVisitor} interface to provide specific action creation methods.
 */
public class LlamaActionCreator implements LlamaActionVisitor {

    private static final String FAILED_TO_SEND_REQUEST_ERROR_MESSAGE = "Failed to send Llama %s request from inference entity id [%s]";
    private static final String COMPLETION_ERROR_PREFIX = "Llama completions";
    private static final String USER_ROLE = "user";

    private static final ResponseHandler EMBEDDINGS_HANDLER = new LlamaEmbeddingsResponseHandler(
        "llama text embedding",
        LlamaEmbeddingsResponseEntity::fromResponse
    );
    private static final ResponseHandler COMPLETION_HANDLER = new LlamaCompletionResponseHandler(
        "llama completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    /**
     * Constructs a new LlamaActionCreator with the specified sender and service components.
     *
     * @param sender the sender to use for executing actions
     * @param serviceComponents the service components providing necessary services
     */
    public LlamaActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(LlamaEmbeddingsModel model) {
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            EMBEDDINGS_HANDLER,
            embeddingsInput -> new LlamaEmbeddingsRequest(
                serviceComponents.truncator(),
                truncate(embeddingsInput.getStringInputs(), model.getServiceSettings().maxInputTokens()),
                model
            ),
            EmbeddingsInput.class
        );

        var errorMessage = buildErrorMessage(TaskType.TEXT_EMBEDDING, model.getInferenceEntityId());
        return new SenderExecutableAction(sender, manager, errorMessage);
    }

    @Override
    public ExecutableAction create(LlamaChatCompletionModel model) {
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            COMPLETION_HANDLER,
            inputs -> new LlamaChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), model),
            ChatCompletionInput.class
        );

        var errorMessage = buildErrorMessage(TaskType.COMPLETION, model.getInferenceEntityId());
        return new SingleInputSenderExecutableAction(sender, manager, errorMessage, COMPLETION_ERROR_PREFIX);
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
