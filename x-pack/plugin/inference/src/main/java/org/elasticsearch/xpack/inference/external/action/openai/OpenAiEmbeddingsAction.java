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
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.openai.OpenAiClient;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequestEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.net.URI;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class OpenAiEmbeddingsAction implements ExecutableAction {

    private final OpenAiAccount account;
    private final OpenAiClient client;
    private final OpenAiEmbeddingsModel model;
    private final String errorMessage;

    public OpenAiEmbeddingsAction(Sender sender, OpenAiEmbeddingsModel model, ServiceComponents serviceComponents) {
        this.model = Objects.requireNonNull(model);
        this.account = new OpenAiAccount(
            this.model.getServiceSettings().uri(),
            this.model.getServiceSettings().organizationId(),
            this.model.getSecretSettings().apiKey()
        );
        this.client = new OpenAiClient(Objects.requireNonNull(sender), Objects.requireNonNull(serviceComponents));
        this.errorMessage = getErrorMessage(this.model.getServiceSettings().uri());
    }

    private static String getErrorMessage(@Nullable URI uri) {
        if (uri != null) {
            return format("Failed to send OpenAI embeddings request to [%s]", uri.toString());
        }

        return "Failed to send OpenAI embeddings request";
    }

    @Override
    public void execute(List<String> input, ActionListener<List<? extends InferenceResults>> listener) {
        try {
            OpenAiEmbeddingsRequest request = new OpenAiEmbeddingsRequest(
                account,
                new OpenAiEmbeddingsRequestEntity(input, model.getTaskSettings().model(), model.getTaskSettings().user())
            );
            ActionListener<List<? extends InferenceResults>> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);

            client.send(request, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }
}
