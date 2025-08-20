/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.action;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceRequestManager;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceResponseHandler;
import org.elasticsearch.xpack.inference.services.huggingface.completion.HuggingFaceChatCompletionModel;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.huggingface.request.completion.HuggingFaceUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.huggingface.request.rerank.HuggingFaceRerankRequest;
import org.elasticsearch.xpack.inference.services.huggingface.rerank.HuggingFaceRerankModel;
import org.elasticsearch.xpack.inference.services.huggingface.response.HuggingFaceElserResponseEntity;
import org.elasticsearch.xpack.inference.services.huggingface.response.HuggingFaceEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.huggingface.response.HuggingFaceRerankResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;

import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the hugging face model type.
 */
public class HuggingFaceActionCreator implements HuggingFaceActionVisitor {

    private static final String FAILED_TO_SEND_REQUEST_ERROR_MESSAGE =
        "Failed to send Hugging Face %s request from inference entity id [%s]";
    private static final String INVALID_REQUEST_TYPE_MESSAGE = "Invalid request type: expected HuggingFace %s request but got %s";
    public static final String COMPLETION_ERROR_PREFIX = "Hugging Face completions";
    static final String USER_ROLE = "user";
    static final ResponseHandler COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
        "hugging face completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );
    private static final ResponseHandler RERANK_HANDLER = new HuggingFaceResponseHandler("hugging face rerank", (request, response) -> {
        if ((request instanceof HuggingFaceRerankRequest) == false) {
            var errorMessage = format(
                INVALID_REQUEST_TYPE_MESSAGE,
                "RERANK",
                request != null ? request.getClass().getSimpleName() : "null"
            );
            throw new IllegalArgumentException(errorMessage);
        }
        return HuggingFaceRerankResponseEntity.fromResponse((HuggingFaceRerankRequest) request, response);
    });

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public HuggingFaceActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(HuggingFaceRerankModel model) {
        var overriddenModel = HuggingFaceRerankModel.of(model, model.getTaskSettings());
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            inputs -> new HuggingFaceRerankRequest(
                inputs.getQuery(),
                inputs.getChunks(),
                inputs.getReturnDocuments(),
                inputs.getTopN(),
                model
            ),
            QueryAndDocsInputs.class
        );
        var errorMessage = buildErrorMessage(TaskType.RERANK, model.getInferenceEntityId());
        return new SenderExecutableAction(sender, manager, errorMessage);
    }

    @Override
    public ExecutableAction create(HuggingFaceEmbeddingsModel model) {
        var responseHandler = new HuggingFaceResponseHandler(
            "hugging face text embeddings",
            HuggingFaceEmbeddingsResponseEntity::fromResponse
        );
        var requestCreator = HuggingFaceRequestManager.of(
            model,
            responseHandler,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var errorMessage = buildErrorMessage(TaskType.TEXT_EMBEDDING, model.getInferenceEntityId());
        return new SenderExecutableAction(sender, requestCreator, errorMessage);
    }

    @Override
    public ExecutableAction create(HuggingFaceElserModel model) {
        var responseHandler = new HuggingFaceResponseHandler("hugging face elser", HuggingFaceElserResponseEntity::fromResponse);
        var requestCreator = HuggingFaceRequestManager.of(
            model,
            responseHandler,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var errorMessage = buildErrorMessage(TaskType.SPARSE_EMBEDDING, model.getInferenceEntityId());
        return new SenderExecutableAction(sender, requestCreator, errorMessage);
    }

    @Override
    public ExecutableAction create(HuggingFaceChatCompletionModel model) {
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            COMPLETION_HANDLER,
            inputs -> new HuggingFaceUnifiedChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), model),
            ChatCompletionInput.class
        );

        var errorMessage = buildErrorMessage(TaskType.COMPLETION, model.getInferenceEntityId());
        return new SingleInputSenderExecutableAction(sender, manager, errorMessage, COMPLETION_ERROR_PREFIX);
    }

    public static String buildErrorMessage(TaskType requestType, String inferenceId) {
        return format(FAILED_TO_SEND_REQUEST_ERROR_MESSAGE, requestType.toString(), inferenceId);
    }
}
