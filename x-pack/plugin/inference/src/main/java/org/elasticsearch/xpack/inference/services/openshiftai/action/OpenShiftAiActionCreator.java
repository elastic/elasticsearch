/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.action;

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
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIResponseHandler;
import org.elasticsearch.xpack.inference.services.jinaai.response.JinaAIRerankResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsResponseHandler;
import org.elasticsearch.xpack.inference.services.openshiftai.request.completion.OpenShiftAiChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openshiftai.request.embeddings.OpenShiftAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.openshiftai.request.rarank.OpenShiftAiRerankRequest;
import org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

/**
 * Creates executable actions for OpenShift AI models.
 * This class implements the {@link OpenShiftAiActionVisitor} interface to provide specific action creation methods.
 */
public class OpenShiftAiActionCreator implements OpenShiftAiActionVisitor {

    private static final String FAILED_TO_SEND_REQUEST_ERROR_MESSAGE =
        "Failed to send OpenShift AI %s request from inference entity id [%s]";
    private static final String COMPLETION_ERROR_PREFIX = "OpenShift AI completions";
    private static final String USER_ROLE = "user";

    private static final ResponseHandler EMBEDDINGS_HANDLER = new OpenShiftAiEmbeddingsResponseHandler(
        "OpenShift AI text embedding",
        OpenAiEmbeddingsResponseEntity::fromResponse
    );
    private static final ResponseHandler COMPLETION_HANDLER = new OpenShiftAiCompletionResponseHandler(
        "OpenShift AI completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );
    // OpenShift AI Rerank task uses the same response format as JinaAI, therefore we can reuse the JinaAIResponseHandler
    private static final ResponseHandler RERANK_HANDLER = new JinaAIResponseHandler(
        "OpenShift AI rerank",
        (request, response) -> JinaAIRerankResponseEntity.fromResponse(response)
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    /**
     * Constructs a new OpenShiftAiActionCreator.
     *
     * @param sender the sender to use for executing actions
     * @param serviceComponents the service components providing necessary services
     */
    public OpenShiftAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(OpenShiftAiEmbeddingsModel model) {
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            EMBEDDINGS_HANDLER,
            embeddingsInput -> new OpenShiftAiEmbeddingsRequest(
                serviceComponents.truncator(),
                truncate(embeddingsInput.getTextInputs(), model.getServiceSettings().maxInputTokens()),
                model
            ),
            EmbeddingsInput.class
        );

        var errorMessage = buildErrorMessage(TaskType.TEXT_EMBEDDING, model.getInferenceEntityId());
        return new SenderExecutableAction(sender, manager, errorMessage);
    }

    @Override
    public ExecutableAction create(OpenShiftAiChatCompletionModel model) {
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            COMPLETION_HANDLER,
            inputs -> new OpenShiftAiChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), model),
            ChatCompletionInput.class
        );

        var errorMessage = buildErrorMessage(TaskType.COMPLETION, model.getInferenceEntityId());
        return new SingleInputSenderExecutableAction(sender, manager, errorMessage, COMPLETION_ERROR_PREFIX);
    }

    @Override
    public ExecutableAction create(OpenShiftAiRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = OpenShiftAiRerankModel.of(model, taskSettings);
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            inputs -> new OpenShiftAiRerankRequest(
                inputs.getQuery(),
                inputs.getChunks(),
                inputs.getReturnDocuments(),
                inputs.getTopN(),
                overriddenModel
            ),
            QueryAndDocsInputs.class
        );
        var errorMessage = buildErrorMessage(TaskType.RERANK, overriddenModel.getInferenceEntityId());
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
