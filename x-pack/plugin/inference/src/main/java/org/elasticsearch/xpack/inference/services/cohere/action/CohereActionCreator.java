/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.action;

import org.elasticsearch.inference.InputType;
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
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.cohere.CohereResponseHandler;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModel;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.cohere.request.v1.CohereV1CompletionRequest;
import org.elasticsearch.xpack.inference.services.cohere.request.v1.CohereV1EmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.cohere.request.v1.CohereV1RerankRequest;
import org.elasticsearch.xpack.inference.services.cohere.request.v2.CohereV2CompletionRequest;
import org.elasticsearch.xpack.inference.services.cohere.request.v2.CohereV2EmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.cohere.request.v2.CohereV2RerankRequest;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankModel;
import org.elasticsearch.xpack.inference.services.cohere.response.CohereCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.cohere.response.CohereEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.cohere.response.CohereRankedResponseEntity;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the cohere model type.
 */
public class CohereActionCreator implements CohereActionVisitor {

    private static final ResponseHandler TEXT_EMBEDDINGS_HANDLER = new CohereResponseHandler(
        "cohere text embedding",
        (request, response) -> CohereEmbeddingsResponseEntity.fromResponse(request, response, TaskType.TEXT_EMBEDDING),
        false
    );

    private static final ResponseHandler IMAGE_EMBEDDINGS_HANDLER = new CohereResponseHandler(
        "cohere image embedding",
        (request, response) -> CohereEmbeddingsResponseEntity.fromResponse(request, response, TaskType.IMAGE_EMBEDDING),
        false
    );

    private static final ResponseHandler MULTIMODAL_EMBEDDINGS_HANDLER = new CohereResponseHandler(
        "cohere multimodal embedding",
        (request, response) -> CohereEmbeddingsResponseEntity.fromResponse(request, response, TaskType.MULTIMODAL_EMBEDDING),
        false
    );

    private static final ResponseHandler RERANK_HANDLER = new CohereResponseHandler(
        "cohere rerank",
        (request, response) -> CohereRankedResponseEntity.fromResponse(response),
        false
    );

    private static final ResponseHandler COMPLETION_HANDLER = new CohereResponseHandler(
        "cohere completion",
        CohereCompletionResponseEntity::fromResponse,
        true
    );

    private static final String COMPLETION_ERROR_PREFIX = "Cohere completion";
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public CohereActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(CohereEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = CohereEmbeddingsModel.of(model, taskSettings);

        Function<EmbeddingsInput, Request> requestCreator = inferenceInputs -> {
            var requestInputType = InputType.isSpecified(inferenceInputs.getInputType())
                ? inferenceInputs.getInputType()
                : overriddenModel.getTaskSettings().getInputType();

            return switch (overriddenModel.getServiceSettings().getCommonSettings().apiVersion()) {
                case V1 -> new CohereV1EmbeddingsRequest(inferenceInputs.getInputs(), requestInputType, overriddenModel);
                case V2 -> new CohereV2EmbeddingsRequest(
                    inferenceInputs.getInputs(),
                    requestInputType,
                    overriddenModel,
                    inferenceInputs.getImageUrls()
                );
            };
        };

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("Cohere embeddings");
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            getResponseHandlerForEmbeddingTaskType(model.getTaskType()),
            requestCreator,
            EmbeddingsInput.class
        );
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    private static ResponseHandler getResponseHandlerForEmbeddingTaskType(TaskType taskType) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> TEXT_EMBEDDINGS_HANDLER;
            case IMAGE_EMBEDDING -> IMAGE_EMBEDDINGS_HANDLER;
            case MULTIMODAL_EMBEDDING -> MULTIMODAL_EMBEDDINGS_HANDLER;
            default -> throw new IllegalArgumentException("Invalid TaskType for embeddings action");
        };
    }

    @Override
    public ExecutableAction create(CohereRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = CohereRerankModel.of(model, taskSettings);

        Function<QueryAndDocsInputs, Request> requestCreator = inferenceInputs -> switch (overriddenModel.getServiceSettings()
            .apiVersion()) {
            case V1 -> new CohereV1RerankRequest(
                inferenceInputs.getQuery(),
                inferenceInputs.getChunks(),
                inferenceInputs.getReturnDocuments(),
                inferenceInputs.getTopN(),
                overriddenModel
            );
            case V2 -> new CohereV2RerankRequest(
                inferenceInputs.getQuery(),
                inferenceInputs.getChunks(),
                inferenceInputs.getReturnDocuments(),
                inferenceInputs.getTopN(),
                overriddenModel
            );
        };

        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            requestCreator,
            QueryAndDocsInputs.class
        );

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("Cohere rerank");
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(CohereCompletionModel model, Map<String, Object> taskSettings) {
        // no overridden model as task settings are always empty for cohere completion model

        Function<ChatCompletionInput, Request> requestCreator = completionInput -> switch (model.getServiceSettings().apiVersion()) {
            case V1 -> new CohereV1CompletionRequest(completionInput.getInputs(), model, completionInput.stream());
            case V2 -> new CohereV2CompletionRequest(completionInput.getInputs(), model, completionInput.stream());
        };

        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            COMPLETION_HANDLER,
            requestCreator,
            ChatCompletionInput.class
        );

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(COMPLETION_ERROR_PREFIX);
        return new SingleInputSenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage, COMPLETION_ERROR_PREFIX);
    }
}
