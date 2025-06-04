/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.util.function.Supplier;

public record ExecutableInferenceRequest(
    RequestSender requestSender,
    Logger logger,
    Request request,
    ResponseHandler responseHandler,
    Supplier<Boolean> hasFinished,
    ActionListener<InferenceServiceResults> listener
) implements Runnable {

    @Override
    public void run() {
        var inferenceEntityId = request.getInferenceEntityId();

        try {
            requestSender.send(logger, request, hasFinished, responseHandler, listener);
        } catch (Exception e) {
            var errorMessage = Strings.format("Failed to send request from inference entity id [%s]", inferenceEntityId);
            logger.warn(errorMessage, e);
            listener.onFailure(new ElasticsearchException(errorMessage, e));
        }
    }
}
