/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudResponseHandler;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudRerankRequest;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.response.TencentCloudRerankResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Creates {@link ExecutableAction}s for TencentCloud embeddings and rerank models.
 * Chat completion is handled directly by the {@code TencentCloudService} via a dedicated request manager (not this visitor).
 */
public class TencentCloudActionCreator implements TencentCloudActionVisitor {

    private static final ResponseHandler EMBEDDINGS_HANDLER = new TencentCloudResponseHandler(
        "tencentcloud text embedding",
        OpenAiEmbeddingsResponseEntity::fromResponse
    );

    private static final ResponseHandler RERANK_HANDLER = new TencentCloudResponseHandler(
        "tencentcloud rerank",
        (request, response) -> TencentCloudRerankResponseEntity.fromResponse(response)
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public TencentCloudActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

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
}
