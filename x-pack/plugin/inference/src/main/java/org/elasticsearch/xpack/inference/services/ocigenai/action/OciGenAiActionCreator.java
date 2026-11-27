/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.action;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.CompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiResponseHandler;
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ocigenai.request.OciGenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.request.OciGenAiRerankRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.request.completion.OciGenAiChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankModel;
import org.elasticsearch.xpack.inference.services.ocigenai.response.OciGenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.ocigenai.response.OciGenAiEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.ocigenai.response.OciGenAiRerankResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

/**
 * Creates the executable actions of the OCI Generative AI models.
 */
public class OciGenAiActionCreator implements OciGenAiActionVisitor {

    static final String COMPLETION_REQUEST_TYPE = "OCI Generative AI completions";
    static final String USER_ROLE = "user";

    private static final ResponseHandler EMBEDDINGS_HANDLER = new OciGenAiResponseHandler(
        "OCI Generative AI embeddings",
        OciGenAiEmbeddingsResponseEntity::fromResponse
    );
    private static final ResponseHandler RERANK_HANDLER = new OciGenAiResponseHandler(
        "OCI Generative AI rerank",
        (request, response) -> OciGenAiRerankResponseEntity.fromResponse(response)
    );
    static final ResponseHandler COMPLETION_HANDLER = new OciGenAiCompletionResponseHandler(
        COMPLETION_REQUEST_TYPE,
        OciGenAiChatCompletionResponseEntity::fromResponseAsCompletion
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public OciGenAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(OciGenAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = OciGenAiEmbeddingsModel.of(model, taskSettings);
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            EMBEDDINGS_HANDLER,
            embeddingsInput -> new OciGenAiEmbeddingsRequest(
                serviceComponents.truncator(),
                truncate(embeddingsInput.getTextInputs(), overriddenModel.getServiceSettings().maxInputTokens()),
                embeddingsInput.getInputType(),
                overriddenModel
            ),
            EmbeddingsInput.class
        );
        var failedToSendRequestErrorMessage = buildErrorMessage(TaskType.TEXT_EMBEDDING, model.getInferenceEntityId());
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(OciGenAiRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = OciGenAiRerankModel.of(model, taskSettings);
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            rerankInput -> new OciGenAiRerankRequest(
                rerankInput.getQueryAsString(),
                rerankInput.getDocsAsStrings(),
                rerankInput.getTopN(),
                rerankInput.getReturnDocuments(),
                overriddenModel
            ),
            QueryAndDocsInputs.class
        );
        var failedToSendRequestErrorMessage = buildErrorMessage(TaskType.RERANK, model.getInferenceEntityId());
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(OciGenAiChatCompletionModel model) {
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            COMPLETION_HANDLER,
            inputs -> new OciGenAiChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), model),
            CompletionInput.class
        );
        var failedToSendRequestErrorMessage = buildErrorMessage(TaskType.COMPLETION, model.getInferenceEntityId());
        return new SingleInputSenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage, COMPLETION_REQUEST_TYPE);
    }

    public static String buildErrorMessage(TaskType requestType, String inferenceId) {
        return format("Failed to send OCI Generative AI %s request from inference entity id [%s]", requestType.toString(), inferenceId);
    }
}
