/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.Request;

public record RunnableRequest(
    BatchingComponents components,
    Logger logger,
    Request request,
    HttpClientContext context,
    ResponseHandler responseHandler,
    ActionListener<InferenceServiceResults> listener
) implements Runnable {

    @Override
    public void run() {
        var requestLine = request.createRequest().getRequestLine();

        try {
            components.requestSender().send(logger, request, context, responseHandler, listener);
        } catch (Exception e) {
            logger.warn(Strings.format("Failed to send request [%s]", requestLine), e);
            listener.onFailure(new ElasticsearchException(Strings.format("Failed to send request [%s]", requestLine), e));
        }
    }
}
