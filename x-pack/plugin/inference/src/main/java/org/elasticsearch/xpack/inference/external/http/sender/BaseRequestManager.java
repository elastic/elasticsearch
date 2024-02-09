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

class BaseRequestManager implements RequestManager {
    protected RequestSender requestSender;

    BaseRequestManager(RequestSender requestSender) {
        this.requestSender = Objects.requireNonNull(requestSender);
    }

    @Override
    public void execute(InferenceRequest inferenceRequest, HttpClientContext context) {
        inferenceRequest.requestCreator().create(inferenceRequest.input(), requestSender, context, inferenceRequest.listener()).run();
    }
}
