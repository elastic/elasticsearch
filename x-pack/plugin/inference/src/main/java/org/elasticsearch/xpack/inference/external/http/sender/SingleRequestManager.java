/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender;

import java.util.Objects;

// TODO remove
/**
 * Handles executing a single inference request at a time.
 */
public class SingleRequestManager {

    protected RetryingHttpSender requestSender;

    public SingleRequestManager(RetryingHttpSender requestSender) {
        this.requestSender = Objects.requireNonNull(requestSender);
    }

    public void execute(InferenceRequest inferenceRequest, HttpClientContext context) {
        if (isNoopRequest(inferenceRequest) || inferenceRequest.hasCompleted()) {
            return;
        }

        inferenceRequest.getRequestManager()
            .execute(
                inferenceRequest.getQuery(),
                inferenceRequest.getInput(),
                requestSender,
                inferenceRequest.getRequestCompletedFunction(),
                inferenceRequest.getListener()
            );
    }

    private static boolean isNoopRequest(InferenceRequest inferenceRequest) {
        return inferenceRequest.getRequestManager() == null
            || inferenceRequest.getInput() == null
            || inferenceRequest.getListener() == null;
    }
}
