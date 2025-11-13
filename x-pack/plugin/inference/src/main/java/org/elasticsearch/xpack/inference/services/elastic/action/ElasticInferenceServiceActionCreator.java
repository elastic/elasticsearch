/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceDenseTextEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceRerankResponseEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceResponseHandler;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceDenseTextEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRerankRequest;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.ELASTIC_INFERENCE_SERVICE_IDENTIFIER;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.extractRequestMetadataFromThreadContext;

public class ElasticInferenceServiceActionCreator implements ElasticInferenceServiceActionVisitor {

    public static final String USER_ROLE = "user";

    static final ResponseHandler DENSE_TEXT_EMBEDDINGS_HANDLER = new ElasticInferenceServiceResponseHandler(
        "elastic dense text embedding",
        ElasticInferenceServiceDenseTextEmbeddingsResponseEntity::fromResponse
    );

    static final ResponseHandler RERANK_HANDLER = new ElasticInferenceServiceResponseHandler(
        "elastic rerank",
        (request, response) -> ElasticInferenceServiceRerankResponseEntity.fromResponse(response)
    );

    static final ResponseHandler COMPLETION_HANDLER = new ElasticInferenceServiceResponseHandler(
        "elastic completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    private final Sender sender;

    private final ServiceComponents serviceComponents;

    private final TraceContext traceContext;

    public ElasticInferenceServiceActionCreator(Sender sender, ServiceComponents serviceComponents, TraceContext traceContext) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
        this.traceContext = traceContext;
    }

    @Override
    public ExecutableAction create(ElasticInferenceServiceSparseEmbeddingsModel model) {
        var requestManager = new ElasticInferenceServiceSparseEmbeddingsRequestManager(model, serviceComponents, traceContext);
        var errorMessage = constructFailedToSendRequestMessage(
            Strings.format("%s sparse embeddings", ELASTIC_INFERENCE_SERVICE_IDENTIFIER)
        );
        return new SenderExecutableAction(sender, requestManager, errorMessage);
    }

    @Override
    public ExecutableAction create(ElasticInferenceServiceRerankModel model) {
        var threadPool = serviceComponents.threadPool();
        var requestManager = new GenericRequestManager<>(
            threadPool,
            model,
            RERANK_HANDLER,
            (rerankInput) -> new ElasticInferenceServiceRerankRequest(
                rerankInput.getQuery(),
                rerankInput.getChunks(),
                rerankInput.getTopN(),
                model,
                traceContext,
                extractRequestMetadataFromThreadContext(threadPool.getThreadContext())
            ),
            QueryAndDocsInputs.class
        );
        var errorMessage = constructFailedToSendRequestMessage(Strings.format("%s rerank", ELASTIC_INFERENCE_SERVICE_IDENTIFIER));
        return new SenderExecutableAction(sender, requestManager, errorMessage);
    }

    @Override
    public ExecutableAction create(ElasticInferenceServiceDenseTextEmbeddingsModel model) {
        var threadPool = serviceComponents.threadPool();

        var manager = new GenericRequestManager<>(
            threadPool,
            model,
            DENSE_TEXT_EMBEDDINGS_HANDLER,
            (embeddingsInput) -> new ElasticInferenceServiceDenseTextEmbeddingsRequest(
                model,
                embeddingsInput.getTextInputs(),
                traceContext,
                extractRequestMetadataFromThreadContext(threadPool.getThreadContext()),
                embeddingsInput.getInputType()
            ),
            EmbeddingsInput.class
        );

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("Elastic dense text embeddings");
        return new SenderExecutableAction(sender, manager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(ElasticInferenceServiceCompletionModel model, Map<String, Object> taskSettings) {
        var threadPool = serviceComponents.threadPool();

        var manager = new GenericRequestManager<>(
            threadPool,
            model,
            COMPLETION_HANDLER,
            (chatCompletionInput) -> new ElasticInferenceServiceUnifiedChatCompletionRequest(
                new UnifiedChatInput(chatCompletionInput.getInputs(), USER_ROLE, false),
                model,
                traceContext,
                extractRequestMetadataFromThreadContext(threadPool.getThreadContext())
            ),
            ChatCompletionInput.class
        );

        var errorMessage = constructFailedToSendRequestMessage(Strings.format("%s completion", ELASTIC_INFERENCE_SERVICE_IDENTIFIER));
        return new SenderExecutableAction(sender, manager, errorMessage);
    }
}
