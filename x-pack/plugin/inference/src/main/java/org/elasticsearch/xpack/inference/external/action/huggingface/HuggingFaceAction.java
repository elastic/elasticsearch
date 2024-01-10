/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.huggingface;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceInferenceRequest;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.common.Truncator.truncate;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class HuggingFaceAction implements ExecutableAction {
    private static final Logger logger = LogManager.getLogger(HuggingFaceAction.class);

    private final HuggingFaceAccount account;
    private final String errorMessage;
    private final RetryingHttpSender sender;
    private final ResponseHandler responseHandler;
    private final Truncator truncator;
    private final Integer tokenLimit;

    public HuggingFaceAction(
        Sender sender,
        HuggingFaceModel model,
        ServiceComponents serviceComponents,
        ResponseHandler responseHandler,
        String requestType
    ) {
        Objects.requireNonNull(serviceComponents);
        Objects.requireNonNull(model);
        Objects.requireNonNull(requestType);

        this.responseHandler = Objects.requireNonNull(responseHandler);

        this.sender = new RetryingHttpSender(
            Objects.requireNonNull(sender),
            serviceComponents.throttlerManager(),
            logger,
            new RetrySettings(serviceComponents.settings()),
            serviceComponents.threadPool()
        );
        this.account = new HuggingFaceAccount(model.getUri(), model.getApiKey());
        this.errorMessage = format("Failed to send Hugging Face %s request to [%s]", requestType, model.getUri().toString());
        this.truncator = Objects.requireNonNull(serviceComponents.truncator());
        this.tokenLimit = model.getTokenLimit();
    }

    @Override
    public void execute(List<String> input, ActionListener<InferenceServiceResults> listener) {
        try {
            var truncatedInput = truncate(input, tokenLimit);

            HuggingFaceInferenceRequest request = new HuggingFaceInferenceRequest(truncator, account, truncatedInput);
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);

            sender.send(request, responseHandler, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }
}
