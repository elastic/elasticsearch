/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

public record ExecutableInferenceRequest(
    RequestSender requestSender,
    Logger logger,
    OutboundRequest outboundRequest,
    ResponseHandler responseHandler,
    TimedListener<InferenceServiceResults> timedListener
) implements Runnable {

    @Override
    public void run() {
        var inferenceEntityId = outboundRequest.getInferenceEntityId();

        try {
            requestSender.send(logger, outboundRequest, responseHandler, timedListener);
        } catch (Exception e) {
            var errorMessage = Strings.format("Failed to send request from inference entity id [%s]", inferenceEntityId);
            logger.warn(errorMessage, e);
            timedListener.getListener().onFailure(new ElasticsearchException(errorMessage, e));
        }
    }
}
