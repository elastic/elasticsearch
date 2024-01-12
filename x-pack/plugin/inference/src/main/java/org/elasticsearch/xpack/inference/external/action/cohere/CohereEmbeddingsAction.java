/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.cohere;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.external.cohere.CohereResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.cohere.CohereEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;

import java.net.URI;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.common.Truncator.truncate;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class CohereEmbeddingsAction implements ExecutableAction {
    private static final Logger logger = LogManager.getLogger(CohereEmbeddingsAction.class);

    private final CohereAccount account;
    private final CohereEmbeddingsModel model;
    private final String errorMessage;
    private final Truncator truncator;
    private final RetryingHttpSender sender;

    public CohereEmbeddingsAction(Sender sender, CohereEmbeddingsModel model, ServiceComponents serviceComponents) {
        this.model = Objects.requireNonNull(model);
        this.account = new CohereAccount(this.model.getServiceSettings().uri(), this.model.getSecretSettings().apiKey());
        this.errorMessage = getErrorMessage(this.model.getServiceSettings().uri());
        this.truncator = Objects.requireNonNull(serviceComponents.truncator());
        this.sender = new RetryingHttpSender(
            Objects.requireNonNull(sender),
            serviceComponents.throttlerManager(),
            logger,
            new RetrySettings(serviceComponents.settings()),
            serviceComponents.threadPool()
        );
    }

    private static String getErrorMessage(@Nullable URI uri) {
        if (uri != null) {
            return format("Failed to send Cohere embeddings request to [%s]", uri.toString());
        }

        return "Failed to send Cohere embeddings request";
    }

    @Override
    public void execute(List<String> input, ActionListener<InferenceServiceResults> listener) {
        try {
            // TODO only truncate if the setting is NONE?
            var truncatedInput = truncate(input, model.getServiceSettings().maxInputTokens());

            CohereEmbeddingsRequest request = new CohereEmbeddingsRequest(truncator, account, truncatedInput, model.getTaskSettings());
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);

            sender.send(request, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }

    private static ResponseHandler createEmbeddingsHandler() {
        return new CohereResponseHandler("cohere text embedding", OpenAiEmbeddingsResponseEntity::fromResponse);
    }
}
