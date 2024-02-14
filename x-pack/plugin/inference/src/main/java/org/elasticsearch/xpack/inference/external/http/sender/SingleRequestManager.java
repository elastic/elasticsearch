/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;

import java.util.Objects;

/**
 * Handles executing a single inference request at a time.
 */
public class SingleRequestManager implements RequestManager {

    public static class Factory implements RequestManagerFactory {

        @Override
        public RequestManager create(RequestSender requestSender) {
            return new SingleRequestManager(requestSender);
        }
    }

    protected RequestSender requestSender;

    SingleRequestManager(RequestSender requestSender) {
        this.requestSender = Objects.requireNonNull(requestSender);
    }

    @Override
    public void execute(InferenceRequest inferenceRequest, HttpClientContext context) {
        if (inferenceRequest.getRequestCreator() == null
            || inferenceRequest.getInput() == null
            || inferenceRequest.getListener() == null
            || inferenceRequest.hasCompleted()) {
            return;
        }

        inferenceRequest.getRequestCreator()
            .create(
                inferenceRequest.getInput(),
                requestSender,
                inferenceRequest.getRequestCompletedFunction(),
                context,
                inferenceRequest.getListener()
            )
            .run();
    }
}
