/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.action;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxRerankRequestManager;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankModel;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * IbmWatsonxActionCreator is responsible for creating executable actions for various models.
 * It implements the IbmWatsonxActionVisitor interface to provide specific implementations.
 */
public class IbmWatsonxActionCreator implements IbmWatsonxActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    static final String COMPLETION_REQUEST_TYPE = "IBM Watsonx completions";
    static final String USER_ROLE = "user";
    static final ResponseHandler COMPLETION_HANDLER = new IbmWatsonxCompletionResponseHandler(
        COMPLETION_REQUEST_TYPE,
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    public IbmWatsonxActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(IbmWatsonxEmbeddingsModel model, Map<String, Object> taskSettings) {
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("IBM Watsonx embeddings");
        return new SenderExecutableAction(
            sender,
            getEmbeddingsRequestManager(model, serviceComponents.truncator(), serviceComponents.threadPool()),
            failedToSendRequestErrorMessage
        );
    }

    @Override
    public ExecutableAction create(IbmWatsonxRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = IbmWatsonxRerankModel.of(model, taskSettings);
        var requestCreator = IbmWatsonxRerankRequestManager.of(overriddenModel, serviceComponents.threadPool());
        var failedToSendRequestErrorMessage = buildErrorMessage(TaskType.RERANK, overriddenModel.getInferenceEntityId());
        return new SenderExecutableAction(sender, requestCreator, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(IbmWatsonxChatCompletionModel chatCompletionModel) {
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            chatCompletionModel,
            COMPLETION_HANDLER,
            inputs -> new IbmWatsonxChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), chatCompletionModel),
            ChatCompletionInput.class
        );

        var failedToSendRequestErrorMessage = buildErrorMessage(TaskType.COMPLETION, chatCompletionModel.getInferenceEntityId());
        return new SingleInputSenderExecutableAction(sender, manager, failedToSendRequestErrorMessage, COMPLETION_REQUEST_TYPE);
    }

    protected IbmWatsonxEmbeddingsRequestManager getEmbeddingsRequestManager(
        IbmWatsonxEmbeddingsModel model,
        Truncator truncator,
        ThreadPool threadPool
    ) {
        return new IbmWatsonxEmbeddingsRequestManager(model, truncator, threadPool);
    }

    /**
     * Builds an error message for IBM Watsonx actions.
     *
     * @param requestType The type of request (e.g. COMPLETION, EMBEDDING, RERANK).
     * @param inferenceId The ID of the inference entity.
     * @return A formatted error message.
     */
    public static String buildErrorMessage(TaskType requestType, String inferenceId) {
        return format("Failed to send IBM Watsonx %s request from inference entity id [%s]", requestType.toString(), inferenceId);
    }
}
