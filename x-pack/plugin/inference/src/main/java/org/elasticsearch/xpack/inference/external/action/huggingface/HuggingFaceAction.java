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
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.batching.HuggingFaceInferenceRequestCreator;
import org.elasticsearch.xpack.inference.external.http.batching.RequestCreator;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class HuggingFaceAction implements ExecutableAction {
    private static final Logger logger = LogManager.getLogger(HuggingFaceAction.class);

    private final String errorMessage;
    private final Sender<HuggingFaceAccount> sender;
    private final RequestCreator<HuggingFaceAccount> requestCreator;

    public HuggingFaceAction(
        Sender<HuggingFaceAccount> sender,
        HuggingFaceModel model,
        ServiceComponents serviceComponents,
        ResponseHandler responseHandler,
        String requestType
    ) {
        Objects.requireNonNull(serviceComponents);
        Objects.requireNonNull(model);
        Objects.requireNonNull(requestType);

        this.sender = Objects.requireNonNull(sender);
        this.errorMessage = format("Failed to send Hugging Face %s request to [%s]", requestType, model.getUri().toString());

        requestCreator = new HuggingFaceInferenceRequestCreator(
            new HuggingFaceAccount(model.getUri(), model.getApiKey()),
            responseHandler,
            serviceComponents.truncator(),
            model.getTokenLimit()
        );
    }

    @Override
    public void execute(List<String> input, ActionListener<InferenceServiceResults> listener) {
        try {
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);
            sender.send(requestCreator, input, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }
}
