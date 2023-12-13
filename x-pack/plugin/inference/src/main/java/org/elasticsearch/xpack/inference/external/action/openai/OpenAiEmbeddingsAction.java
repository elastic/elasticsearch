/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.openai;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.batching.OpenAiEmbeddingsRequestCreator;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.openai.OpenAiResponseHandler;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequestEntity;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.net.URI;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class OpenAiEmbeddingsAction implements ExecutableAction {

    private static final ResponseHandler EMBEDDINGS_HANDLER = new OpenAiResponseHandler(
        "openai text embedding",
        OpenAiEmbeddingsResponseEntity::fromResponse
    );

    private final OpenAiAccount account;
    private final OpenAiEmbeddingsModel model;
    private final String errorMessage;
    private final Sender<OpenAiAccount> sender;

    public OpenAiEmbeddingsAction(Sender<OpenAiAccount> sender, OpenAiEmbeddingsModel model, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.model = Objects.requireNonNull(model);
        this.account = new OpenAiAccount(
            this.model.getServiceSettings().uri(),
            this.model.getServiceSettings().organizationId(),
            this.model.getSecretSettings().apiKey()
        );
        this.errorMessage = getErrorMessage(this.model.getServiceSettings().uri());
    }

    private static String getErrorMessage(@Nullable URI uri) {
        if (uri != null) {
            return format("Failed to send OpenAI embeddings request to [%s]", uri.toString());
        }

        return "Failed to send OpenAI embeddings request";
    }

    @Override
    public void execute(List<String> input, ActionListener<InferenceServiceResults> listener) {
        try {
            OpenAiEmbeddingsRequest request = new OpenAiEmbeddingsRequest(
                account,
                new OpenAiEmbeddingsRequestEntity(input, model.getTaskSettings().model(), model.getTaskSettings().user())
            );
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);

            sender.send(new OpenAiEmbeddingsRequestCreator(model, account, EMBEDDINGS_HANDLER), input, wrappedListener);

            // client.send(request, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }
}
