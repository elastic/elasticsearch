/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.action;

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
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudResponseHandler;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudRerankRequest;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.response.TencentCloudRerankResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Creates {@link ExecutableAction}s for TencentCloud embeddings, rerank, and completion models using the visitor pattern.
 * Unified chat completion (the {@code chat_completion} task type) is handled directly by the
 * {@code TencentCloudService#doUnifiedCompletionInfer} path.
 */
public class TencentCloudActionCreator implements TencentCloudActionVisitor {

    private static final String COMPLETION_ERROR_PREFIX = "TencentCloud completions";
    private static final String USER_ROLE = "user";

    private static final ResponseHandler EMBEDDINGS_HANDLER = new TencentCloudResponseHandler(
        "tencentcloud text embedding",
        OpenAiEmbeddingsResponseEntity::fromResponse
    );

    private static final ResponseHandler RERANK_HANDLER = new TencentCloudResponseHandler(
        "tencentcloud rerank",
        (request, response) -> TencentCloudRerankResponseEntity.fromResponse(response)
    );

    private static final ResponseHandler COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
        "tencentcloud completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    /**
     * Constructs a new TencentCloudActionCreator with the specified sender and service components.
     *
     * @param sender the sender to use for executing actions
     * @param serviceComponents the service components providing necessary services
     */
    public TencentCloudActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    /**
     * Creates an executable action for the given TencentCloud embeddings model.
     *
     * @param model the TencentCloud embeddings model
     * @param taskSettings the task-level settings for this request
     * @return an executable action for the embeddings model
     */
    @Override
    public ExecutableAction create(TencentCloudEmbeddingsModel model, Map<String, Object> taskSettings) {
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            EMBEDDINGS_HANDLER,
            (embeddingsInput) -> new TencentCloudEmbeddingsRequest(embeddingsInput.getTextInputs(), model),
            EmbeddingsInput.class
        );
        return new SenderExecutableAction(sender, requestManager, constructFailedToSendRequestMessage("TencentCloud embeddings"));
    }

    /**
     * Creates an executable action for the given TencentCloud rerank model, applying any rerank task settings
     * overrides carried by the request.
     *
     * @param model the TencentCloud rerank model
     * @param taskSettings the task-level settings for this request, used to override the model's rerank task settings
     * @return an executable action for the rerank model
     */
    @Override
    public ExecutableAction create(TencentCloudRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = TencentCloudRerankModel.of(model, taskSettings);
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            (rerankInput) -> new TencentCloudRerankRequest(
                rerankInput.getQueryAsString(),
                rerankInput.getDocsAsStrings(),
                rerankInput.getReturnDocuments(),
                rerankInput.getTopN(),
                overriddenModel
            ),
            QueryAndDocsInputs.class
        );
        return new SenderExecutableAction(sender, requestManager, constructFailedToSendRequestMessage("TencentCloud rerank"));
    }

    /**
     * Creates an executable action for the given TencentCloud chat completion model.
     *
     * @param model the TencentCloud chat completion model
     * @param taskSettings the task-level settings for this request
     * @return an executable action for the chat completion model
     */
    @Override
    public ExecutableAction create(TencentCloudChatCompletionModel model, Map<String, Object> taskSettings) {
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            COMPLETION_HANDLER,
            (chatCompletionInput) -> new TencentCloudChatCompletionRequest(new UnifiedChatInput(chatCompletionInput, USER_ROLE), model),
            ChatCompletionInput.class
        );
        var errorMessage = constructFailedToSendRequestMessage(COMPLETION_ERROR_PREFIX);
        return new SingleInputSenderExecutableAction(sender, requestManager, errorMessage, COMPLETION_ERROR_PREFIX);
    }
}
